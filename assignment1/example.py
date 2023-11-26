
# import libraries
import pandas as pd
import sys
import argparse


def read_data(source):
    df = pd.read_csv(source)
    return df


def transform_data(data):
    transformed_data = data.copy()
    transformed_data[['Month', 'Year']] = transformed_data['MonthYear'].str.split(' ', expand=True)
    transformed_data[['Name']] = transformed_data[['Name']].fillna('Name_less')
    transformed_data[['Outcome Subtype']] = transformed_data[['Outcome Subtype']].fillna('Not_Available')
    transformed_data.drop(['MonthYear'], axis=1, inplace = True)
    return transformed_data


def export_data(new_data, target):
    new_data.to_csv(target)


if __name__ == "__main__":
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument('source', help='source csv')
    args_parser.add_argument('target', help=('target file'))
    args = args_parser.parse_args()

    print("Start processing....")
    data = read_data(args.source)
    new_data = transform_data(data)
    export_data(new_data, args.target)
    print("Finished processing.....")

