import nipyapi.security
import nipyapi.canvas
import nipyapi.config
import nipyapi.utils
import nipyapi.templates
import urllib3


def main():
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # disable TLS check, do at your own risk
    nipyapi.config.nifi_config.verify_ssl = False
    nipyapi.config.registry_config.verify_ssl = False

    # connect to Nifi
    nipyapi.utils.set_endpoint("https://localhost:8443/nifi-api")
    # wait for connection to be set up
    connected = nipyapi.utils.wait_to_complete(
        test_function=nipyapi.utils.is_endpoint_up,
        endpoint_url="https://localhost:8443/nifi",
        nipyapi_delay=nipyapi.config.long_retry_delay,
        nipyapi_max_wait=nipyapi.config.short_max_wait
    )

    login = nipyapi.security.service_login(
        service='nifi', username='', password='', bool_response=True)

    nipyapi.canvas.schedule_process_group(
        process_group_id='root', scheduled=True)
    print('ok')


main()
