import mysql.connector
from datetime import datetime, timezone
import pytz

# MySQL connection
con = mysql.connector.connect(
    host='localhost',
    user='root',
    password='actowiz',
    database='vision_11_bot'
)
cur = con.cursor()


# Convert epoch to datetime object
def epoch_to_datetime(epoch_time):
    ist = pytz.timezone('Asia/Kolkata')
    return datetime.fromtimestamp(int(epoch_time), tz=ist)


def get_backup_contests(start_date, end_date):
    try:
        start_datetime = epoch_to_datetime(start_date)
        end_datetime = epoch_to_datetime(end_date)

        # Convert datetime objects to the format expected by MySQL
        start_date_str = start_datetime.strftime('%Y-%m-%d %H:%M:%S')
        end_date_str = end_datetime.strftime('%Y-%m-%d %H:%M:%S')

        cur.execute('SELECT * FROM backup_contests WHERE match_start_datetime BETWEEN %s AND %s',
                    (start_date_str, end_date_str))
        data = cur.fetchall()

        if data:
            columns = [desc[0] for desc in cur.description]
            data_dict = [dict(zip(columns, row)) for row in data]
            result = {'status': 200, 'message': 'Data retrieved successfully.', 'data': data_dict}
        else:
            result = {'status': 404, 'message': 'No data found for the given date range.', 'data': None}

        return result
    except:
        return {'status': 500, 'message': 'Internal server error', 'data': None}


if __name__ == '__main__':
    print(get_backup_contests('1722533400', '1722879000'))
