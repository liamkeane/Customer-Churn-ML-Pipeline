import logging
import pandas as pd

log = logging.getLogger(__name__)

class ETLProcessor:
    def __init__(self, csv_path: str, engine):
        self.csv_path = csv_path
        self.engine = engine

    def calculate_statistics(self, df):
        log.info("Calculating statistics...")
        # Calculate average price
        average_price = df["price"].mean()
        log.info(f"Average book price: {average_price}")

        # Calculate median price
        median_price = df["price"].median()
        log.info(f"Median book price: {median_price}")

        # Calculate total books
        total_books = df.shape[0]
        log.info(f"Total number of books: {total_books}")

        # Calculate total authors
        total_authors = df["author_id"].nunique()
        log.info(f"Total number of authors: {total_authors}")

        # Load statistics into the database
        stats_df = pd.DataFrame({
            "average_price": [average_price],
            "median_price": [median_price],
            "total_books": [total_books],
            "total_authors": [total_authors]
        })
        return stats_df

    def extract(self):
        # informational logging
        log.info(f"Extracting data from {self.csv_path}")
        return pd.read_csv(self.csv_path)

    def process(self):
        df = self.extract()
        self.load(df)

    def load(self, df):
        # create unique authors into the `author` table
        author_uq_series = df["author_name"].unique()
        author_df = pd.DataFrame(author_uq_series, columns=["author_name"])
        author_df.to_sql("author", con=self.engine, index=False, if_exists="append")
        log.info(f"Loaded {len(author_df)} authors into the database.")

        # create unique formats into the `format` table
        format_uq_series = df["format"].unique()
        format_df = pd.DataFrame(format_uq_series, columns=["format"])
        format_df.rename(columns={"format": "format_name"}, inplace=True)
        format_df.to_sql("format", con=self.engine, index=False, if_exists="append")
        log.info(f"Loaded {len(format_df)} book formats into the database.")

        # merge author and format data with books
        author_df = pd.read_sql("SELECT * FROM author", con=self.engine)
        format_df = pd.read_sql("SELECT * FROM format", con=self.engine)
        df_merged = pd.merge(df, author_df, on="author_name")
        df_merged = pd.merge(df_merged, format_df, left_on="format", right_on="format_name")

        # load books into the `books` table
        df_merged = df_merged[["author_id", "book_title", "format_id", "publish_date", "price"]]
        df_merged.to_sql("books", con=self.engine, index=False, if_exists="append")
        log.info(f"Loaded {len(df_merged)} books into the database.")

        # Calculate and load statistics
        stats_df = self.calculate_statistics(df_merged)
        stats_df.to_sql("central_tendency", con=self.engine, index=False, if_exists="append")
