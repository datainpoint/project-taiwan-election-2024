import numpy as np
import pandas as pd
import warnings

warnings.simplefilter("ignore")

class TidyTaiwanElection2024Data:
    def __init__(self):
        self.county_names = ['連江縣', '屏東縣', '臺南市', '雲林縣', '基隆市', '新北市', '新竹市', '宜蘭縣', '嘉義縣', '臺東縣', '臺北市', '彰化縣', '嘉義市', '新竹縣', '金門縣', '苗栗縣', '南投縣', '臺中市', '花蓮縣', '高雄市', '澎湖縣', '桃園市']
        self.region_column_names = ["town", "village", "polling_place"]
        self.office_column_names = ["effective_votes", "wasted_votes", "voted", "issued_not_voted", "issued_votes", "remained_votes", "number_of_voters", "vote_rate"]
    def tidy_president_dataframes(self, df: pd.core.frame.DataFrame, county_name: str) -> tuple:
        number_of_columns = df.shape[1]
        candidates_info = list(df.columns.values[3:(number_of_columns - 8)])
        column_names = self.region_column_names + candidates_info + self.office_column_names
        df.columns = column_names
        df["town"] = df["town"].str.replace("\u3000", "")
        df["town"] = df["town"].fillna(method="ffill")
        df = df.dropna()
        df["polling_place"] = df["polling_place"].astype(int)
        region_data = df.iloc[:, [0, 1, 2]]
        office_data = df.iloc[:, -8::]
        candidate_data = df.loc[:, candidates_info]
        office_df = pd.concat((region_data, office_data), axis=1)
        number_of_rows = office_df.shape[0]
        county_names = [county_name for _ in range(number_of_rows)]
        election_types = ["總統副總統" for _ in range(number_of_rows)]
        regions = [np.NaN for _ in range(number_of_rows)]
        office_df.insert(0, "region", regions)
        office_df.insert(0, "county", county_names)
        number_of_columns = office_df.shape[1]
        office_df.insert(number_of_columns, "election_type", election_types)
        candidate_df = pd.concat((region_data, candidate_data), axis=1)
        candidate_df.insert(0, "county", county_names)
        melted_candidate_df = pd.melt(candidate_df,
                                      id_vars=["county", "town", "village", "polling_place"],
                                      var_name="number_candidate",
                                      value_name="votes")
        split_number_candidate = melted_candidate_df["number_candidate"].str.split("\n")
        numbers = [int(elem[0].replace("(", "").replace(")", "")) for elem in split_number_candidate]
        candidates = [f"{elem[1]}/{elem[2]}" for elem in split_number_candidate]
        melted_candidate_df = melted_candidate_df.drop("number_candidate", axis=1)
        number_of_columns = melted_candidate_df.shape[1]
        parties = []
        for candidate in candidates:
            if candidate == "柯文哲/吳欣盈":
                parties.append("台灣民眾黨")
            elif candidate == "賴清德/蕭美琴":
                parties.append("民主進步黨")
            else:
                parties.append("中國國民黨")
        melted_candidate_df.insert(number_of_columns - 1, "number", numbers)
        melted_candidate_df.insert(number_of_columns, "party", parties)
        melted_candidate_df.insert(number_of_columns + 1, "candidate", candidates)
        return melted_candidate_df, office_df
    def tidy_legislator_dataframes(self, df: pd.core.frame.DataFrame, county_name: str, region_name: str, legislator_type: str) -> tuple:
        number_of_columns = df.shape[1]
        candidates_info = list(df.columns.values[3:(number_of_columns - 8)])
        column_names = self.region_column_names + candidates_info + self.office_column_names
        df.columns = column_names
        df["town"] = df["town"].str.replace("\u3000", "")
        df["town"] = df["town"].fillna(method="ffill")
        df = df.dropna()
        df["polling_place"] = df["polling_place"].astype(int)
        region_data = df.iloc[:, [0, 1, 2]]
        office_data = df.iloc[:, -8::]
        candidate_data = df.loc[:, candidates_info]
        office_df = pd.concat((region_data, office_data), axis=1)
        number_of_rows = office_df.shape[0]
        county_names = [county_name for _ in range(number_of_rows)]
        if legislator_type == "區域立委":
            region_names = [region_name for _ in range(number_of_rows)]
        else:
            region_names = [np.NaN for _ in range(number_of_rows)]
        legislator_types = [legislator_type for _ in range(number_of_rows)]
        office_df.insert(0, "region", region_names)
        office_df.insert(0, "county", county_names)
        number_of_columns = office_df.shape[1]
        office_df.insert(number_of_columns, "election_type", legislator_types)
        candidate_df = pd.concat((region_data, candidate_data), axis=1)
        candidate_df.insert(0, "region", region_names)
        candidate_df.insert(0, "county", county_names)
        melted_candidate_df = pd.melt(candidate_df,
                                      id_vars=["county", "region", "town", "village", "polling_place"],
                                      var_name="number_candidate",
                                      value_name="votes")
        split_number_candidate = melted_candidate_df["number_candidate"].str.split("\n")
        numbers = [int(elem[0].replace("(", "").replace(")", "")) for elem in split_number_candidate]
        candidates = [elem[1] for elem in split_number_candidate]
        parties = [elem[2] for elem in split_number_candidate]
        legislator_types = [legislator_type for _ in split_number_candidate]
        melted_candidate_df = melted_candidate_df.drop("number_candidate", axis=1)
        number_of_columns = melted_candidate_df.shape[1]
        melted_candidate_df.insert(number_of_columns - 1, "number", numbers)
        melted_candidate_df.insert(number_of_columns, "party", parties)
        melted_candidate_df.insert(number_of_columns + 1, "candidate", candidates)
        melted_candidate_df.insert(number_of_columns + 3, "legislator_type", legislator_types)
        return melted_candidate_df, office_df
    def tidy_party_legislator_dataframes(self, df: pd.core.frame.DataFrame, county_name: str, legislator_type: str) -> tuple:
        number_of_columns = df.shape[1]
        candidates_info = list(df.columns.values[3:(number_of_columns - 8)])
        column_names = self.region_column_names + candidates_info + self.office_column_names
        df.columns = column_names
        df["town"] = df["town"].str.replace("\u3000", "")
        df["town"] = df["town"].fillna(method="ffill")
        df = df.dropna()
        df["polling_place"] = df["polling_place"].astype(int)
        region_data = df.iloc[:, [0, 1, 2]]
        office_data = df.iloc[:, -8::]
        candidate_data = df.loc[:, candidates_info]
        office_df = pd.concat((region_data, office_data), axis=1)
        number_of_rows = office_df.shape[0]
        county_names = [county_name for _ in range(number_of_rows)]
        legislator_types = [legislator_type for _ in range(number_of_rows)]
        regions = [np.NaN for _ in range(number_of_rows)]
        office_df.insert(0, "region", regions)
        office_df.insert(0, "county", county_names)
        number_of_columns = office_df.shape[1]
        office_df.insert(number_of_columns, "election_type", legislator_types)
        candidate_df = pd.concat((region_data, candidate_data), axis=1)
        candidate_df.insert(0, "county", county_names)
        melted_candidate_df = pd.melt(candidate_df,
                                      id_vars=["county", "town", "village", "polling_place"],
                                      var_name="number_party",
                                      value_name="votes")
        split_number_party = melted_candidate_df["number_party"].str.split("\n")
        numbers = [int(elem[0].replace("(", "").replace(")", "")) for elem in split_number_party]
        parties = [elem[2] for elem in split_number_party]
        legislator_types = [legislator_type for _ in split_number_party]
        melted_candidate_df = melted_candidate_df.drop("number_party", axis=1)
        number_of_columns = melted_candidate_df.shape[1]
        melted_candidate_df.insert(number_of_columns - 1, "number", numbers)
        melted_candidate_df.insert(number_of_columns, "party", parties)
        melted_candidate_df.insert(number_of_columns + 2, "legislator_type", legislator_types)
        return melted_candidate_df, office_df
    def concat_president_dataframes(self) -> tuple:
        concatenated_president_df = pd.DataFrame()
        concatenated_office_df = pd.DataFrame()
        for county_name in self.county_names:
            file_name = f"總統-各投票所得票明細及概況(Excel檔)/總統-A05-4-候選人得票數一覽表-各投開票所({county_name}).xlsx"
            df = pd.read_excel(file_name,
                               engine="openpyxl",
                               skiprows=[0, 1, 3, 4, 5],
                               thousands=",")
            tidy_president_df, tidy_office_df = self.tidy_president_dataframes(df, county_name)
            concatenated_president_df = pd.concat((concatenated_president_df, tidy_president_df))
            concatenated_president_df = concatenated_president_df.reset_index(drop=True)
            concatenated_office_df = pd.concat((concatenated_office_df, tidy_office_df))
            concatenated_office_df = concatenated_office_df.reset_index(drop=True)
            print(f"Tidying {file_name}......")
        return concatenated_president_df, concatenated_office_df
    def concat_legislator_dataframes(self) -> tuple:
        legislator_types = {"區域立委": 2,
                            "不分區立委": 6,
                            "山地立委": 4,
                            "平地立委": 4}
        concatenated_legislator_df = pd.DataFrame()
        concatenated_party_legislator_df = pd.DataFrame()
        concatenated_office_df = pd.DataFrame()
        for county_name in self.county_names:
            for k, v in legislator_types.items():
                file_name = f"立委各投開票所得票數一覽表/{k}-A05-{v}-得票數一覽表({county_name}).xlsx"
                excel_file = pd.ExcelFile(file_name)
                sheet_names = excel_file.sheet_names
                for sheetName in sheet_names:
                    df = pd.read_excel(file_name,
                                       sheet_name=sheetName,
                                       engine="openpyxl",
                                       skiprows=[0, 1, 3, 4, 5],
                                       thousands=",")
                    if k == "不分區立委":
                        tidy_party_legislator_df, tidy_party_legislator_office_df = self.tidy_party_legislator_dataframes(df, county_name, k)
                        concatenated_party_legislator_df = pd.concat((concatenated_party_legislator_df, tidy_party_legislator_df))
                        concatenated_party_legislator_df = concatenated_party_legislator_df.reset_index(drop=True)
                        concatenated_office_df = pd.concat((concatenated_office_df, tidy_party_legislator_office_df))
                        concatenated_office_df = concatenated_office_df.reset_index(drop=True)
                    else:
                        tidy_legislator_df, tidy_legislator_office_df = self.tidy_legislator_dataframes(df, county_name, sheetName, k)
                        concatenated_legislator_df = pd.concat((concatenated_legislator_df, tidy_legislator_df))
                        concatenated_legislator_df = concatenated_legislator_df.reset_index(drop=True)
                        concatenated_office_df = pd.concat((concatenated_office_df, tidy_legislator_office_df))
                        concatenated_office_df = concatenated_office_df.reset_index(drop=True)
                print(f"Tidying {file_name}......")
        concatenated_legislator_df["votes"] = concatenated_legislator_df["votes"].astype(int)
        astype_dict = {"effective_votes": int,
                       "wasted_votes": int,
                       "voted": int,
                       "issued_not_voted": int,
                       "issued_votes": int,
                       "remained_votes": int,
                       "number_of_voters": int}
        concatenated_office_df = concatenated_office_df.astype(astype_dict)
        return concatenated_legislator_df, concatenated_party_legislator_df, concatenated_office_df
    def get_tidy_dataframes(self):
        president_df, president_office_df = self.concat_president_dataframes()
        legislator_df, party_legislator_df, legislator_office_df = self.concat_legislator_dataframes()
        concatenated_office_df = pd.concat((president_office_df, legislator_office_df))
        concatenated_office_df = concatenated_office_df.reset_index(drop=True)
        tidy_dataframes = {
            "president": president_df,
            "legislator": legislator_df,
            "party_legislator": party_legislator_df,
            "polling_place": concatenated_office_df
        }
        self._tidy_dataframes = tidy_dataframes
        return tidy_dataframes
    def get_tidy_csv_files(self):
        self._tidy_dataframes["president"].to_csv("president.csv", index=False)
        self._tidy_dataframes["legislator"].to_csv("legislator.csv", index=False)
        self._tidy_dataframes["party_legislator"].to_csv("party_legislator.csv", index=False)
        self._tidy_dataframes["polling_place"].to_csv("polling_place.csv", index=False)