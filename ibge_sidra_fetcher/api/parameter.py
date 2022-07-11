import re
from typing import Any, Optional

URL = "https://apisidra.ibge.gov.br/values"


class Parameter:

    # Agregado
    aggregate: str
    # 1 => /t/1

    # Variáveis
    variables: list[str]
    # ["123", "1234"] => /v/123,1234
    # [] => /v/all
    # ["all"] => /v/all

    # Nível territorial
    territories: dict[str, list[str]]
    # {"6": ["3304557", "3550308"]} => /n6/3304557,3550308
    # {"1": ["all"], "6": []} => /n1/all/n6/all

    # Períodos
    periods: list[str]
    # ["201201", "201301-201312"] => /p/201201,201301-201312
    # [] => /p/all
    # ["all"] => /p/all

    # Classificações
    classifications: dict[str, list[str]]
    # {"1": ["123", "321"], "32": ["23"]} => /c1/123,321/c32/23
    # {"1": ["all"], "32": []} => /c1/all/c32/all

    # Precisão
    decimals: str
    # /d/m

    def __init__(
        self,
        aggregate: str,
        territories: dict[str, list[str]],
        variables: list[str],
        periods: list[str],
        classifications: dict[str, list[str]],
        decimals: Optional[str] = "/d/m",       # Padrão é precisão máxima
    ) -> None:
        self.aggregate = aggregate
        self.territories = territories
        self.variables = variables
        self.periods = periods
        self.classifications = classifications
        self.decimals = decimals

    def assign(self, name: str, value: Any) -> "Parameter":
        p = Parameter(
            aggregate=self.aggregate,
            territories=self.territories,
            variables=self.variables,
            periods=self.periods,
            classifications=self.classifications,
            decimals=self.decimals,
        )
        setattr(p, name, value)
        return p

    def url(self) -> str:

        t = f"/t/{self.aggregate}"  # Agregado

        n = ""
        for key, value in self.territories.items():
            if len(value) > 0:
                n = n + f"/n{key}/" + ",".join(value)
            else:
                n = n + f"/n{key}/all"

        if len(self.variables) > 0:
            v = "/v/" + ",".join(self.variables)
        else:
            v = "/v/all"

        if len(self.periods) > 0:
            p = "/p/" + ",".join(self.periods)
        else:
            p = "/p/all"

        c = ""
        for key, value in self.classifications.items():
            if len(value) > 0:
                c = c + f"/c{key}/" + ",".join(value)
            else:
                c = c + f"/c{key}/all"

        d = "/d/m"  # Precisão máxima

        return URL + t + n + v + p + c + d

    def __repr__(self) -> str:
        return self.url()

    def __str__(self) -> str:
        return self.url()

    def __eq__(self, o: "Parameter") -> bool:
        aggregate = self.aggregate == o.aggregate
        territories = self.territories == o.territories
        variables = self.variables == o.variables
        periods = self.periods == o.periods
        classifications = self.classifications == o.classifications
        decimals = self.decimals == o.decimals
        return (
            aggregate
            and territories
            and variables
            and periods
            and classifications
            and decimals
        )


def get_sidra_url_request_period(
    parameter: Parameter,
    period_id: int,
) -> str:
    p = parameter.assign("periods", [str(period_id)])
    url = p.url()
    return url


def parse_territories(url: str) -> dict[str, list[str]]:
    n = re.findall(r"(\/n\d\/)(all|\d+(,\d+)*)", url)
    n = ["".join(g) for g in n]
    territories = {
        ter.strip("n"): select.split(",")
        for _, ter, select in [i.split("/") for i in n]
    }
    return n, territories


def parse_periods(url):
    p = re.search(r"/p/(all|(first|last(%20\d+|))|\d{6}(,\d{6})*)", url)
    p = p.group()
    periods = p.strip("/p").split(",")
    return p, periods


def parse_decimal(url):
    d = re.search(r"\/d\/(v\d+%20\d+)(,v\d+%20\d+)*", url)
    decimal = {}
    if d:
        d = d.group()
        decimal = {
            i.split("%20")[0].strip("v"): i.split("%20")[1]
            for i in d.strip("/d").split(",")
        }

    return d, decimal


def parse_variables(url):
    v = re.search(r"(\/v\/)(all|allxp|\d+(,\d+)*)", url)
    variables = []
    if v:
        v = v.group()
        variables = v.strip("/v").split(",")
    return v, variables


def parse_classifications(url):
    c = re.findall(r"(\/c\d+\/)(all|allxt|\d+(,\d+)*)", url)
    c = ["".join(g) for g in c]
    classifications = {
        cat.strip("c"): [select]
        for _, cat, select in [i.split("/") for i in c]
    }

    return c, classifications


def parse_aggregate(url):
    t = re.search(r"/t/\d+", url)
    t = t.group()
    aggregate = t.strip("/t")
    return t, aggregate


def parse_url(url: str) -> dict[str, str]:
    """
    {
        "url": url,
        "t": t,
        "aggregate": aggregate,
        "n": n,
        "territories": territories,
        "c": c,
        "classifications": classifications,
        "v": v,
        "variables": variables,
        "d": d,
        "decimal": decimal,
        "p": p,
        "periods": periods,
    }
    """
    url = url.replace(URL, "")  # Remove the base of the url

    t, aggregate = parse_aggregate(url)
    n, territories = parse_territories(url)
    c, classifications = parse_classifications(url)
    v, variables = parse_variables(url)
    d, decimal = parse_decimal(url)
    p, periods = parse_periods(url)

    return {
        "url": url,
        "t": t,
        "aggregate": aggregate,
        "n": n,
        "territories": territories,
        "c": c,
        "classifications": classifications,
        "v": v,
        "variables": variables,
        "d": d,
        "decimal": decimal,
        "p": p,
        "periods": periods,
    }


def parameter_from_url(url: str) -> Parameter:
    _, aggregate = parse_aggregate(url)
    _, territories = parse_territories(url)
    _, classifications = parse_classifications(url)
    _, variables = parse_variables(url)
    _, decimal = parse_decimal(url)
    _, periods = parse_periods(url)
    parameter = Parameter(
        aggregate=aggregate,
        territories=territories,
        variables=variables,
        periods=periods,
        classifications=classifications,
        decimals=decimal,
    )
    return parameter
