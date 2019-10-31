import os
import traceback

import azure.functions as func
import pandas as pd
import snowflake.connector
from msrestazure.azure_active_directory import MSIAuthentication
from azure.keyvault import KeyVaultClient
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    GET API to retrieve the usage statistics from Snowflake on all warehouses (exposed in view).

    Args:
        req: request packet

    Returns: response with usage statistics in JSON document - stored as entries per warehouse.

    """

    try:
        logger.info('INFO: Create raw dataframe from snowflake')
        df = get_dataframe()

        logger.info('INFO: Compute aggregations')
        aggregated_df = get_aggregated_dataframe(df=df)

        return func.HttpResponse(f"{aggregated_df.T.to_json()}!")

    except Exception as ex:

        err_msg = "Error while retrieving and processing data from. Please try again later. " + traceback.format_exc()
        logger.error(err_msg)
        return func.HttpResponse( err_msg, status_code=400)


def parse_connection_secret(secret: str) -> dict:
    """
    Parse the connection information from the secret

    Args:
        secret: Parses a 'secret' with format: user|password|account|warehouse|database|schema|role

    Returns: dictionary with parsed connection information

    """

    split_secret = secret.split("|")

    return {
        "user": split_secret[0],
        "password": split_secret[1],
        "account": split_secret[2],
        "warehouse": split_secret[3],
        "database": split_secret[4],
        "schema": split_secret[5],
        "role": split_secret[6],
    }


def get_dataframe() -> pd.DataFrame:
    """
    Uses pandas to pull the data from a query into into a pandas dataframe

    Returns: pandas dataframe with the extracted data

    """

    logger.info('INFO: Create sqlalchemy engine')

    sec = parse_connection_secret(get_secret('secretuser'))
    con = snowflake.connector.connect(
        user=sec['user'],
        password=sec['password'],
        account=sec['account'],
        warehouse=sec['warehouse'],
        database=sec['database'],
        schema=sec['schema'],
        role=sec['role'],
    )

    df = pd.read_sql('select * from usage_v', con)
    df.columns = [col.lower() for col in df.columns]

    logger.info(df.columns)

    return df


def get_aggregated_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute aggregations of the warehouse usage: min, max, sum, count of hours with usage and average of usage over
    hours with usage

    Args:
        df: dataframe with at least the two columns warehouse_name and credits_used.

    Returns: aggregated pandas Dataframe

    """

    aggregated_df = df.groupby('warehouse_name').agg({
        'credits_used': {'mean_over_hours_of_usage': 'mean',
                         'sum_usage': 'sum',
                         'min_usage': 'min',
                         'max_usage': 'max',
                         'hours_with_usage': 'count'}
    })

    # Remove the multiindex primary column name
    aggregated_df.columns = aggregated_df.columns.droplevel(0)

    return aggregated_df


def get_secret(conf_value) -> str:

    secret = os.getenv(conf_value)

    if len(secret.split("|")) == 7:
            return secret
    elif len(secret.split("|")) == 3:
            return get_secret_from_keyvault(resource_packet=secret)
    else:
        raise ValueError('secret was not in application settings')


def get_secret_from_keyvault(resource_packet: str) -> str:
    """
    Passes the information necessary to access the secret value as URI:ID:Version

    Args:
        resource_packet: Informnation packet with contents used for retrieving secret for KeyVault.

    Returns: extracted secret value for the specified version of the secret

    """
    # This method will raise an exception on failure. It has not been hardened in any way.

    logger.info('INFO: Attempting to retrieve secret from KeyVault')

    # Create MSI Authentication
    credentials = MSIAuthentication()

    # Obtain hooks into the KeyVault - establish a client
    client = KeyVaultClient(credentials)

    resource_uri, secret_id, secret_version = resource_packet.split("|")

    # Retrieve the secret
    secret_bundle = client.get_secret(resource_uri, secret_id, secret_version)

    logger.info('INFO: Secret successfully retrieved from KeyVault')

    # The secret is stored in a value field. Retrieve this and return to the caller
    return secret_bundle.value
