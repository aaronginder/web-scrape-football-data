import re
import os
import requests
import pandas as pd
from bs4 import BeautifulSoup as soup


class FootballWebCrawler:
    def __init__(self) -> None:
        """
        Initializes the FootballWebCrawler class. This class collects current and historic football for the Premier League.
        """
        self._base_download_url = "https://www.football-data.co.uk"
        self._football_data_url = self._generate_premier_league_url()
        self.union_df = pd.DataFrame()

    def _generate_premier_league_url(self) -> str:
        """
        Generates the complete Premier League URL to retrieve data from.

        :return: the URL value
        :rtype: str
        """
        return f"{self._base_download_url}/englandm.php"

    def _submit_website_request(self) -> requests.models.Response:
        """
        Submits a request to football-data and returns a response object.

        :return: a response object from https://www.football-data.co.uk/englandm.php
        :rtype: requests.models.Response
        """
        return requests.get(self._football_data_url)

    @staticmethod
    def _parse_website_response(response: requests.models.Response) -> soup:
        """
        Parses a website response as lxml.

        :param response: the response object of the html request to https://wwww.football-data.co.uk
        :type response: requests.models.Response
        :return: a soup lxml object of the response content
        :rtype: soup
        """
        return soup(response.content, "lxml")

    @staticmethod
    def find_all_hyperlinks(html_code: soup) -> list:
        """
        Retrieves all a tags and there respective tree within a html response object that contains hyperlinks.

        :param html_code: a soup object containing the lxml of the http request to https://www.football-data.co.uk
        :type html_code: soup
        :return: list of hyperlinks in the html code
        :rtype: list
        """
        return [a["href"] for a in html_code.find_all("a", href=True)]

    @staticmethod
    def _extract_football_resource(hyperlinks: list) -> list:
        """
        Extracts all csv file names that match the regex: mmz4282/\d{4}/E0. The E0 refers to the Premier League

        :param hyperlinks: a list of the hyperlinks
        :type hyperlinks: list
        :return: a condensed list of hyperlinks that match the regex for the csv file name on football-data
        :rtype: list
        """
        resource_links = []
        for link in hyperlinks:
            if re.findall(r"mmz4281/\d{4}/E0", link) != []:
                resource_links.append(link)
        return resource_links

    def _compose_resource_urls(self, links: list) -> list:
        """
        Creates the URI object to download a specific file. This takes the base link for football-data and concatenates the csv link.

        :param links: List of csv file names for the URI links
        :type links: list
        :return: A list of URL objects to download csv files
        :rtype: list
        """
        return [f"{self._base_download_url}/{resource}" for resource in links]

    def _extract_football_csv_links(self) -> list:
        """
        A wrapper class that runs each of the functions to extract data from https://www.football-data.co.uk, the csv file names and compiles into a single list of links.

        :return: list of football csv file URLS
        :rtype: list
        """
        response = self._submit_website_request()
        html = self._parse_website_response(response)
        hyperlinks = self.find_all_hyperlinks(html)
        links = self._extract_football_resource(hyperlinks)
        urls = self._compose_resource_urls(links)

        return urls

    def _drop_empty_rows(self) -> None:
        self.union_df.dropna(subset=["HomeTeam", "AwayTeam"], inplace=True)

    def process_football_csv_to_output(
        self,
        csv_or_dict: str = "dict",
        output_folder: str = None,
        number_of_football_seasons: int = None,
    ) -> pd.core.frame.DataFrame:
        """
        Processes all the football comma delimited file containing data for each season in the Premier League and compiles into a single DataFrame object.

        :param csv_or_dict: select an output type as csv and pass a output folder path, or dict will return a dict object
        :param output_folder: name of the output folder where the CSV output will be stored
        :type output_folder: str
        :param number_of_football_seasons: the number of football seasons since the current season to process, defaults to None
        :type number_of_football_seasons: int, optional
        :return: a DataFrame object with the data of all comma split value files
        :rtype: pd.core.frame.DataFrame
        """

        if number_of_football_seasons is None:
            number_of_football_seasons = 15

        links = self._extract_football_csv_links()
        condensed_links = links[0:number_of_football_seasons]
        for link in condensed_links:
            csv = pd.read_csv(link, parse_dates=["Date"])
            self.union_df = self.union_df._append(csv, ignore_index=True)
            print(f"PROCESS: successfully loaded {link}")

        self._drop_empty_rows()
        path = os.getcwd() + f"/{output_folder}/EPL.csv"
        try:
            if csv_or_dict.lower() == "csv":
                self.union_df.to_csv(path, index=False)
                print(f"SAVE: Saved file as csv to: {path}")
            elif csv_or_dict.lower() == "dict":
                return self.union_df.to_dict()
            else:
                raise Exception
        except Exception:
            print(f"Error: could not output dataframe to {path}")
        return self.union_df
