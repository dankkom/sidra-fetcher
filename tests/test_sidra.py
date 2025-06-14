from sidra_fetcher.api.sidra import (
    Parametro,
    parameter_from_url,
    parse_aggregate,
    parse_classifications,
    parse_decimal,
    parse_periods,
    parse_territories,
    parse_url,
    parse_variables,
)

url = "https://apisidra.ibge.gov.br/values/t/6723/n1/all/v/all/p/all/c844/all/d/v1394%202,v1395%202,v1396%202,v10008%205"


def test_parse_aggregate():
    t, aggregate = parse_aggregate(url)
    assert t == "/t/6723"
    assert aggregate == "6723"


def test_parse_territories():
    n, territories = parse_territories(url)
    assert n == ["/n1/all"]
    assert territories == {"1": ["all"]}


def test_parse_classifications():
    c, classifications = parse_classifications(url)
    assert c == ["/c844/all"]
    assert classifications == {"844": ["all"]}


def test_parse_variables():
    v, variables = parse_variables(url)
    assert v == "/v/all"
    assert variables == ["all"]


def test_parse_decimal():
    d, decimal = parse_decimal(url)
    assert d == "/d/v1394%202,v1395%202,v1396%202,v10008%205"
    assert decimal == {"1394": "2", "1395": "2", "1396": "2", "10008": "5"}


def test_parse_periods():
    p, periods = parse_periods(url)
    assert p == "/p/all"
    assert periods == ["all"]


def test_parse_url():
    parsed = parse_url(url)
    assert (
        parsed["url"]
        == "/t/6723/n1/all/v/all/p/all/c844/all/d/v1394%202,v1395%202,v1396%202,v10008%205"
    )
    assert parsed["t"] == "/t/6723"
    assert parsed["aggregate"] == "6723"
    assert parsed["n"] == ["/n1/all"]
    assert parsed["territories"] == {"1": ["all"]}
    assert parsed["c"] == ["/c844/all"]
    assert parsed["classifications"] == {"844": ["all"]}
    assert parsed["v"] == "/v/all"
    assert parsed["variables"] == ["all"]
    assert parsed["d"] == "/d/v1394%202,v1395%202,v1396%202,v10008%205"
    assert parsed["decimal"] == {
        "1394": "2",
        "1395": "2",
        "1396": "2",
        "10008": "5",
    }
    assert parsed["p"] == "/p/all"
    assert parsed["periods"] == ["all"]


def test_parameter_from_url():
    parameter = parameter_from_url(url)
    assert isinstance(parameter, Parametro)
    assert parameter.agregado == "6723"
    assert parameter.territorios == {"1": ["all"]}
    assert parameter.variaveis == ["all"]
    assert parameter.periodos == ["all"]
    assert parameter.classificacoes == {"844": ["all"]}
    assert parameter.decimais == {
        "1394": "2",
        "1395": "2",
        "1396": "2",
        "10008": "5",
    }


if __name__ == "__main__":
    test_parse_aggregate()
    test_parse_territories()
    test_parse_classifications()
    test_parse_variables()
    test_parse_decimal()
    test_parse_periods()
    test_parse_url()
    test_parameter_from_url()
    print("All tests passed!")
