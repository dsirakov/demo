## Installation
1. Install Python 3.7 with pip
2. Install [pipenv](https://pipenv.kennethreitz.org/en/latest/install/#installing-pipenv])
3. While in projects folder 
    ```bash
    pipenv sync
    pipenv shell
    ```
## Usage
1. Export environmenn
    ```bash
    export SNOWFLAKE_USR=usr
    export SNOWFLAKE_PWD=pass
    ```
    Where _usr_ and _pass_ are provided in the email.

2. Run script
    ```bash
    python -m viademo
    ```