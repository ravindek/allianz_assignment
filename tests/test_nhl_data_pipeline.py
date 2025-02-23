"""
Unit tests for the ETL task.
"""
import unittest
from unittest.mock import AsyncMock, patch, MagicMock

import aiohttp

from source.nhl_data_pipeline import fetch_page, get_pages_url, get_html_table, save_to_excel, \
    transform


class TestScraper(unittest.IsolatedAsyncioTestCase):
    """
    Unit tests for the ETL task.
    """
    @patch("aiohttp.ClientSession.get")
    async def test_fetch_page(self, mock_get):
        """
        Test fetch_page without making actual HTTP requests.
        """
        mock_response = AsyncMock()
        mock_response.text.return_value = "<html><body>Test Page</body></html>"
        mock_get.return_value.__aenter__.return_value = mock_response

        async with aiohttp.ClientSession() as session:
            result = await fetch_page(session, "http://example.com")

        self.assertEqual(result, "<html><body>Test Page</body></html>")

    @patch("source.nhl_data_pipeline.fetch_page")
    async def test_get_pages_url(self, mock_fetch_page):
        """
        Test get_pages_url with mocked HTML.
        """
        mock_html = '''
        <html>
            <ul class="pagination">
                <li><a href="/pages/forms/?page=1">1</a></li>
                <li><a href="/pages/forms/?page=2">2</a></li>
                <li><a href="/pages/forms/?page=3">3</a></li>
            </ul>
        </html>
        '''
        mock_fetch_page.return_value = mock_html

        async with aiohttp.ClientSession() as session:
            result = await get_pages_url(session)

        expected_pages = ["/pages/forms/?page=1", "/pages/forms/?page=2", "/pages/forms/?page=3"]
        self.assertEqual(result, expected_pages)

    def test_get_html_table(self):
        """
        Test extracting table data from HTML.
        """
        mock_html = '''
        <html>
            <table class="table">
                <tr><th>Team</th><th>Wins</th></tr>
                <tr><td>Boston Bruins</td><td>44</td></tr>
                <tr><td>Chicago Blackhawks</td><td>30</td></tr>
            </table>
        </html>
        '''
        rows, headers = get_html_table(mock_html, [])

        expected_headers = ["Team", "Wins"]
        expected_rows = [["Boston Bruins", "44"], ["Chicago Blackhawks", "30"]]

        self.assertEqual(headers, expected_headers)
        self.assertEqual(rows, expected_rows)

    @patch("source.nhl_data_pipeline.Workbook")
    def test_save_to_excel(self, mock_workbook):
        """
        Test saving data to Excel.
        """
        mock_ws = MagicMock()
        mock_wb = MagicMock()
        mock_wb.active = mock_ws
        mock_workbook.return_value = mock_wb

        rows = [["Boston Bruins", "44"], ["Chicago Blackhawks", "30"]]
        headers = ["Team", "Wins"]
        save_to_excel(rows, headers)

        mock_ws.append.assert_any_call(headers)
        mock_ws.append.assert_any_call(["Boston Bruins", "44"])
        mock_ws.append.assert_any_call(["Chicago Blackhawks", "30"])
        mock_wb.save.assert_called_once()

    @patch("source.nhl_data_pipeline.load_workbook")
    def test_transform(self, mock_load_workbook):
        """
        Test transforming data in Excel.
        """
        mock_ws = MagicMock()
        mock_ws.iter_rows.return_value = [(1990, "Boston Bruins", 44), (1990, "Chicago Blackhawks", 30)]
        # Mocking the header row
        mock_ws.__getitem__.return_value = [MagicMock(value="Year"), MagicMock(value="Team Name"),
                                            MagicMock(value="Wins")]
        mock_wb = MagicMock()
        mock_wb.__getitem__.return_value = mock_ws
        mock_load_workbook.return_value = mock_wb

        transform()

        mock_wb.create_sheet.assert_called_with("Winner and Loser per Year")
        mock_wb.save.assert_called_once()


if __name__ == "__main__":
    unittest.main()
