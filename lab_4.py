from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
import httpx
from datetime import timedelta

@task(retries = 3, retry_delay_seconds = 1, cache_key_fn=task_input_hash,
        cache_expiration = timedelta(seconds=60))
def fetch_weather(lat, long):
    logger = get_run_logger()
    result = httpx.get(f'https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={long}&hourly=temperature_2m,windspeed_10m')
    return result

@task(persist_result=True)
def parse_response(data):
    logger = get_run_logger()
    payload = data.json()
    return payload

@task
def calc_average_temperature(data):
    logger = get_run_logger()
    hourly_data = data['hourly']
    avg_temp = sum(hourly_data['temperature_2m']) / len(hourly_data['temperature_2m'])
    return(avg_temp)

@task
def calc_average_windspeed(data):
    logger = get_run_logger()
    hourly_data = data['hourly']
    avg_ws = sum(hourly_data['windspeed_10m']) / len(hourly_data['windspeed_10m'])
    return(avg_ws)

@flow
def print_avg_windspeed(lat, long):
    logger = get_run_logger()
    res = fetch_weather(lat, long)
    data = parse_response(res)
    ws = calc_average_windspeed(data)
    logger.info(f'Average Windspeed = {ws}')


@flow
def print_avg_temperature(lat, long):
    logger = get_run_logger()
    res = fetch_weather(lat, long)
    data = parse_response(res)
    temp = calc_average_temperature(data)
    logger.info(f'Average Temperature = {temp}')

@flow
def print_weather(lat = 52, long = 18):
    logger = get_run_logger()
    logger.debug('Getting Average Temp and Windspeed (10m)')
    print_avg_temperature(lat, long)
    print_avg_windspeed(lat, long)

if __name__ == '__main__':
    print_weather(lat = 49, long = 18)