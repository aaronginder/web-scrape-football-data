import numpy as np
import pandas as pd
from config.column_mapping import normal_mapping, reverse_mapping, drop_columns


class FootballDataParser:
    def __init__(
        self,
        ingress_file_path: str,
        file_format: str = "csv",
    ) -> None:
        """
        Initialises the class.

        :param ingress_file_path: the full ingress file path. This file should be a csv. Must not include the extension. Path can be relative to the workspace folder or the full path from root.
        :type ingress_file_path: str
        :param file_format: the extension of the ingress and egress file, defaults to "csv"
        :type file_format: str, optional
        """
        self.df = pd.read_csv(f"{ingress_file_path}.{file_format}")
        self.file_format = file_format

    def _reverse_data(self) -> pd.core.frame.DataFrame:
        """
        Reverses the headings and values from away and home. This is to process the table so that one row represents a team in a match. Currently, the data structure shows one row per game.

        :return: a DataFrame object with the headings and values reversed (e.g. team becomes opponent and respective values reverse)
        :rtype: pd.core.frame.DataFrame
        """

        df = self.df.rename(columns=reverse_mapping)
        return df

    @staticmethod
    def _append_data(
        dataframe: pd.core.frame.DataFrame,
        append_dataframe: pd.core.frame.DataFrame,
    ) -> pd.core.frame.DataFrame:
        """
        Appends data on to the bottom of a DataFrame

        :param dataframe: the dataframe to append to
        :type dataframe: pd.core.frame.DataFrame
        :param append_dataframe: the dataframe you wish to append to 'dataframe'
        :type append_dataframe: pd.core.frame.DataFrame
        :return: a DataFrame object with the appended values assigned to the respective columns
        :rtype: pd.core.frame.DataFrame
        """
        return pd.concat([dataframe, append_dataframe])

    def _rename_normal_column_headings(self) -> pd.core.frame.DataFrame:
        """
        Renames the short abbreviation headings to a complete, human-interpretted heading

        :return: a DataFrame object with the new column headings
        :rtype: pd.core.frame.DataFrame
        """
        return self.df.rename(columns=normal_mapping)

    @staticmethod
    def _drop_columns_with_no_mapping(
        dataframe: pd.core.frame.DataFrame,
    ) -> pd.core.frame.DataFrame:
        """
        Drops the columns that have no mapping and therefore, cannot be interpretted.

        :param dataframe: the dataframe object
        :type dataframe: pd.core.frame.DataFrame
        :return: a condensed DataFrame
        :rtype: pd.core.frame.DataFrame
        """
        return dataframe.drop(drop_columns, axis=1)

    def _drop_empty_rows_in_multiple_dataframes(self, dataframe_1, dataframe_2):
        dropped_na_dataframe_1 = self._drop_columns_with_no_mapping(dataframe_1)
        dropped_na_dataframe_2 = self._drop_columns_with_no_mapping(dataframe_2)

        return dropped_na_dataframe_1, dropped_na_dataframe_2

    @staticmethod
    def _add_full_time_result_flag(
        dataframe: pd.core.frame.DataFrame,
    ) -> pd.core.frame.DataFrame:
        dataframe["full_time_results_difference"] = (
            dataframe["full_time_team_goals"] - dataframe["full_time_opponent_goals"]
        )
        for value in dataframe["full_time_results_difference"]:
            if value > 0:
                dataframe["full_time_result"] = "team"
            elif value == 0:
                dataframe["full_time_result"] = "draw"
            elif value < 0:
                dataframe["full_time_result"] = "opponent"

        return dataframe

    @staticmethod
    def _add_half_time_result_flag(
        dataframe: pd.core.frame.DataFrame,
    ) -> pd.core.frame.DataFrame:
        dataframe["half_time_results_difference"] = (
            dataframe["half_time_team_goals"] - dataframe["half_time_opponent_goals"]
        )
        for value in dataframe["half_time_results_difference"]:
            if value > 0:
                dataframe["half_time_result"] = "team"
            elif value == 0 and value is not None:
                dataframe["half_time_result"] = "draw"
            elif value < 0:
                dataframe["half_time_result"] = "opponent"

        return dataframe

    def _add_flags_to_dataframes(
        self,
        renamed_dataframe: pd.core.frame.DataFrame,
        reversed_dataframe: pd.core.frame.DataFrame,
    ) -> pd.core.frame.DataFrame:
        home_away_renamed_df, home_away_reversed_df = self._add_home_or_away_flag(
            renamed_dataframe=renamed_dataframe, reversed_dataframe=reversed_dataframe
        )
        home_away_renamed_df_ft = self._add_full_time_result_flag(home_away_renamed_df)
        home_away_reversed_df_ft = self._add_full_time_result_flag(
            home_away_reversed_df
        )
        home_away_renamed_df_ht = self._add_half_time_result_flag(
            home_away_renamed_df_ft
        )
        home_away_reversed_df_ht = self._add_half_time_result_flag(
            home_away_reversed_df_ft
        )

        return home_away_renamed_df_ht, home_away_reversed_df_ht

    @staticmethod
    def _add_home_or_away_flag(
        renamed_dataframe: pd.core.frame.DataFrame,
        reversed_dataframe: pd.core.frame.DataFrame,
    ) -> pd.core.frame.DataFrame:
        """
        Adds the 'team_home_away_flag' column to the dataframe. For data that hasn't been reversed, the values in the team column are home. For values in the reversed dataframe, called 'new_dataframe', these are the away teams.

        :param renamed_dataframe: the original dataframe with no changes to the columns
        :type renamed_dataframe: pd.core.frame.DataFrame
        :param new_dataframe: the reversed dataframe with the new reversed mapping columns
        :type reversed_dataframe: pd.core.frame.DataFrame
        :return: Two dataframes: the first is the renamed dataframe and second is the reversed with the home/away flag
        :rtype: pd.core.frame.DataFrame
        """
        renamed_dataframe["team_home_away_flag"] = "home"
        reversed_dataframe["team_home_away_flag"] = "away"

        return renamed_dataframe, reversed_dataframe

    def flatten_and_rename_dataframe(self) -> pd.core.frame.DataFrame:
        """
        Processes the raw DataFrame object by renaming column headings, reversing data and union to create a flat, tabular data structure compatible with Tableau, and drops any headings that cannot be interpretted.

        :return: [description]
        :rtype: pd.core.frame.DataFrame
        """
        reversed_df = self._reverse_data()
        renamed_df = self._rename_normal_column_headings()
        (
            dropped_renamed_df,
            dropped_reversed_df,
        ) = self._drop_empty_rows_in_multiple_dataframes(
            dataframe_1=renamed_df, dataframe_2=reversed_df
        )
        flagged_renamed_df, flagged_reversed_df = self._add_flags_to_dataframes(
            renamed_dataframe=dropped_renamed_df, reversed_dataframe=dropped_reversed_df
        )
        appended_df = self._append_data(flagged_renamed_df, flagged_reversed_df)
        
        return appended_df

    def process_df_and_output_as_csv(self, csv_egress_path: str) -> None:
        """
        Processes  the football-data dataframe object and outputs object as a CSV. Wrapper function for the flatten_and_rename_dataframe method.

        :param csv_egress_path:full or relative path to the output file. Must include the file name
        :type csv_egress_path: str
        :return: None
        :rtype: None
        """
        df = self.flatten_and_rename_dataframe()
        df.to_csv(f"{csv_egress_path}.{self.file_format}", index=False)

        return None
