import sys
from viademo.utils import get_data, env_setup, \
                          load_to_db, build_snapshot


def main():

    config = env_setup()

    datasource = config['datasource']
    status_file = config['status_file']
    output_file = config['output_file']
    snowflake = config['snowflake']

    data = get_data(datasource, status_file)

    build_snapshot(data, output_file)

    load_to_db(snowflake, output_file)


if __name__ == "__main__":
    sys.exit(main())
