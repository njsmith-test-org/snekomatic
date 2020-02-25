import asks


async def test_main_smoke(our_app_url):
    response = await asks.get(our_app_url)
    assert "Hi!" in response.text
