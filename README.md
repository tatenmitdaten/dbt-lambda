# Project

The package provides helper functions to run dbt on AWS Lambda. The dbt project is fetched from a Git repository hosted on GitHub or AWS CodeCommit. The package does not include the actual app and samconfig files but only provides reusables helper functions.

# How to configure

- `DBT_DOCS_BUCKET` - The S3 bucket where the dbt documentation is stored if `dbt docs generate` is run. You can provide a simple API Gateway to access the documentation with a secure connection.
- `DBT_REPOSITORY_NAME` - The name of the dbt repository.
- `GITHUB_ACCESS_TOKEN` - The GitHub access token to access the repository if the repository is hosted on GitHub.
- `GITHUB_SECRET_ARN` - The ARN of the secret that stores the GitHub access token on AWS Secret Manager. If the `GITHUB_SECRET_ARN` is provided, the `GITHUB_ACCESS_TOKEN` is overwritten with the value stored in the secret.
- `CODECOMMIT_ROLE_ARN` - The ARN of the role that has access to the AWS CodeCommit repository if the repository is hosted on AWS CodeCommit. The role is only required if the CodeCommit repository is in a separate AWS account.
- `SNOWFLAKE_SECRET_ARN` - The ARN of the secret that stores the Snowflake credentials on AWS Secret Manager.

Best practice is to store all parameters in the samconfig.yaml file and ship it with the project. The advantage is that the parameters need to be defined only in one place. The values are used in the sam template when creating the stack. The app can read the values from the samconfig file and set the environment variables.
