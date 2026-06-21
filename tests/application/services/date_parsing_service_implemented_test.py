from application.services.dates.date_parsing_service_implemented import SystemDateTimeServiceImplemented

def test_execute_returns_fixed_datetime():
    service = SystemDateTimeServiceImplemented()

    assert service.utc_now_compact()
