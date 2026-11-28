# -*- coding: utf-8 -*-
"""E6: mapper que emite (barra, importe) por cada compra.

Entrada (CSV):  compra_id,data_hora,barra,pulseira,produto,unidades,importe
Saída:           barra<TAB>importe

Funciona con Python 2.7 (o do clúster) e con Python 3.
"""
from __future__ import print_function
import sys

for linha in sys.stdin:
    campos = linha.strip().split(",")
    if len(campos) != 7 or campos[0] == "compra_id":
        continue
    barra = campos[2]
    importe = campos[6]
    print("%s\t%s" % (barra, importe))