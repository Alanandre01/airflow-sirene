import os, csv
os.makedirs("tests/fixtures", exist_ok=True)

header = ["SIRET","SIREN","ETAT_ETABLISSEMENT","CODE_POSTAL","DATE_CREATION_ETAB"]

valid = [
    ["12345678901234","123456789","Actif","44000","2020-01-15"],
    ["98765432109876","987654321","Fermé","75001","2018-06-30"],
    ["11111111111111","111111111","Actif","69001","2019-03-10"],
    ["22222222222222","222222222","Fermé","33000","2021-07-01"],
    ["33333333333333","333333333","Actif","44300","2020-05-20"],
    ["44444444444444","444444444","Fermé","31000","2017-11-12"],
    ["55555555555555","555555555","Actif","13001","2022-02-28"],
    ["66666666666666","666666666","Actif","67000","2019-09-15"],
    ["77777777777777","777777777","Fermé","59000","2016-04-01"],
    ["88888888888888","888888888","Actif","06000","2023-01-10"],
    ["99999999999999","999999999","Actif","34000","2021-12-31"],
    ["10000000000001","100000000","Fermé","38000","2018-08-08"],
]
# NULL structurel : reproduit l'anomalie réelle (ETAT_ETABLISSEMENT/CODE_POSTAL/
# DATE_CREATION_ETAB ~75% NULL, cf. create_suite.py) — SIRET/SIREN restent
# renseignés, ce ne sont pas des colonnes affectées par l'anomalie côté prod.
nulls = [
    [f"{20000000000000 + i:014d}", f"{200000000 + i:09d}", "", "", ""]
    for i in range(36)
]
invalids = [
    ["1234567890",    "123456789","Actif","44000","2020-01-15"],
    ["123456789012345","123456789","Fermé","75001","2018-06-30"],
]
rows = valid + nulls + invalids

with open("tests/fixtures/sirene_fixture.csv","w",newline="",encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(header)
    w.writerows(rows)
print(f"Fixture: {len(rows)} lignes — {len(valid)} valides, "
      f"{len(nulls)} NULL, {len(invalids)} SIRET invalides")
