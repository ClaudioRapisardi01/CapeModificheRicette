from time import sleep

import DB

#Declare
array_cdCapecchi=[] #array per la gestione dei codici ricetta presenti nel db Ricette dell app CapeApp
array_cdMTS=[] #array per la gestione dei codici ricetta presenti nel db di MTS
array_nuoveRicette=[] #array per la gestione delle nuove ricette
array_obsoleteRicette=[] #array per la gestione delle ricette diventate obsolete
query_mts_ricetta = """SELECT * 
  FROM [MTS].[Origine].[vRicette]
  where CdRecipe=? and DRecipe = ?"""
query_mts_ing=""""""
query_capecchi_ricetta = """SELECT *
  FROM [Ricette].[Application].[ItemsDetails]
  where CDrecipe=? and Cd_Ar=?"""
query_capecchu_ing=""""""

def aggiornamentoRicetta(CDricetta,CDArtcicolo):
    query=""



print("inizio controllo modifiche ricette")

#download lista di tutte le ricette nel nostro sistema

query="""select * from ricette.application.items where Gestione='A'"""
ricetteCapecchi=DB.ReadData(query,())
print("Ricette Scaricate")

#downloaddi tutte le ricette da mts
query2="""SELECT * FROM [MTS].[Origine].[vRicette] where DtRecipeObsoleta is null"""
ricetteMTS=DB.ReadData(query,())
print("MTS Scaricate")


for rc in ricetteCapecchi:
    array_cdCapecchi.append(rc[1])

for rc in ricetteMTS:
    array_cdMTS.append(rc[1])

for rc in ricetteCapecchi:
    if rc[1] not in array_cdMTS:
        array_obsoleteRicette.append(rc[1])

for rc in ricetteMTS:
    if rc[1] not in array_cdCapecchi:
        array_obsoleteRicette.append(rc[1])

print("Ricette Nuove    ", len(array_nuoveRicette))
print("Ricette Obsolete ", len(array_obsoleteRicette))

for rc in ricetteCapecchi:
    if rc[1] not in array_obsoleteRicette:
        ricettaMTS=DB.ReadData(query_mts_ricetta,(rc[1],rc[2]))
        ingredientiCapecchi=DB.ReadData(query_capecchi_ricetta,(rc[1],rc[2]))
        if len(ricettaMTS)>len(ingredientiCapecchi):
            print("ricetta: ",rc[1],"  nuovo ingrediente")
        if len(ricettaMTS)<len(ingredientiCapecchi):
            print("ricetta: ",rc[1],"ingrediente rimosso")
        if len(ricettaMTS)==len(ingredientiCapecchi):
            print("ok")










#fine del processo di gestione delle modifiche


print("Processo terminato")


