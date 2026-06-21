from application.services.data_preparation.dates import SystemDateTimeServiceImplemented

def test_execute_returns_fixed_datetime():
    service = SystemDateTimeServiceImplemented()

    assert service.utc_now_compact()
