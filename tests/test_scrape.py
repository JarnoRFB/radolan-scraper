import radolan_scraper.scrape


def test_extract_filenames():
    table = """
            <html>
            <head><title>Index of /climate_environment/CDC/grids_germany/hourly/radolan/historical/asc/2006/</title></head>
            <body bgcolor="white">
            <h1>Index of /climate_environment/CDC/grids_germany/hourly/radolan/historical/asc/2006/</h1><hr><pre><a href="../">../</a>
            <a href="RW-200601.tar">RW-200601.tar</a>                                      28-Jun-2019 12:00            15349760
            <a href="RW-200602.tar">RW-200602.tar</a>                                      28-Jun-2019 12:00            24596480
            <a href="RW-200603.tar">RW-200603.tar</a>                                      28-Jun-2019 12:00            37478400
            <a href="RW-200604.tar">RW-200604.tar</a>                                      28-Jun-2019 12:00            38502400
            <a href="RW-200605.tar">RW-200605.tar</a>                                      28-Jun-2019 12:00            44247040
            <a href="RW-200606.tar">RW-200606.tar</a>                                      28-Jun-2019 12:00            26408960
            <a href="RW-200607.tar">RW-200607.tar</a>                                      28-Jun-2019 12:00            28303360
            <a href="RW-200608.tar">RW-200608.tar</a>                                      28-Jun-2019 12:00            64768000
            <a href="RW-200609.tar">RW-200609.tar</a>                                      28-Jun-2019 12:00            20654080
            <a href="RW-200610.tar">RW-200610.tar</a>                                      28-Jun-2019 12:00            34211840
            <a href="RW-200611.tar">RW-200611.tar</a>                                      28-Jun-2019 12:00            35153920
            <a href="RW-200612.tar">RW-200612.tar</a>                                      28-Jun-2019 12:00            26378240
            </pre><hr></body>
            </html>
            """

    extracted = radolan_scraper.scrape.extract_filenames(table)
    assert extracted == [
        "RW-200601.tar",
        "RW-200602.tar",
        "RW-200603.tar",
        "RW-200604.tar",
        "RW-200605.tar",
        "RW-200606.tar",
        "RW-200607.tar",
        "RW-200608.tar",
        "RW-200609.tar",
        "RW-200610.tar",
        "RW-200611.tar",
        "RW-200612.tar",
    ]

