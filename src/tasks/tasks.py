import luigi
from config.conf import tasks_config
from utils.web_crawler import FootballWebCrawler
from utils.data_parser import FootballDataParser


class GetFootballDataFromWebsite(luigi.Task):
    """
    Retrieves football csv data from https://www.football-data.co.uk, combines into a single DataFrame and outputs as a CSV file to the output folder specified in config/conf.py.
    """

    success = False

    @staticmethod
    def requires():
        return []

    def run(self):
        scraper = FootballWebCrawler()
        scraper.process_football_csv_to_output(
            csv_or_dict=tasks_config["files"]["format"],
            output_folder=tasks_config["files"]["output_folder"],
            # number_of_football_seasons=8,
        )
        self.success = True

    def complete(self):
        return self.success


class ProcessFootballData(luigi.Task):
    """
    Takes the raw, combined football CSV season and betting data, flattens the data from one row per match to one row per team for each match; cleans and renames headings to non-abbreviated terms and adds flags to the data for analysis.
    """

    success = False

    @staticmethod
    def requires():
        return [GetFootballDataFromWebsite()]

    def run(self):
        task_name = self.get_task_family()
        parser = FootballDataParser(
            ingress_file_path=tasks_config[task_name]["ingress_file_path"],
            file_format=tasks_config["files"]["format"],
        )
        parser.process_df_and_output_as_csv(
            csv_egress_path=tasks_config[task_name]["egress_file_path"]
        )

    def complete(self):
        return self.success


if __name__ == "__main__":
    luigi.build(tasks=[GetFootballDataFromWebsite()], local_scheduler=True)
