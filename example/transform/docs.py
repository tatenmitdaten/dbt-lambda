import dbt_lambda.docs


def lambda_handler(event, context):
    return dbt_lambda.docs.lambda_handler(event, context)
