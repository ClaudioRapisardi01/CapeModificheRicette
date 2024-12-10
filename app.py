from time import sleep

import DB

#Declare
array_cdCapecchi=[] #array per la gestione dei codici ricetta presenti nel db Ricette dell app CapeApp
array_cdMTS=[] #array per la gestione dei codici ricetta presenti nel db di MTS
array_nuoveRicette=[] #array per la gestione delle nuove ricette
array_obsoleteRicette=[] #array per la gestione delle ricette diventate obsolete
query_mts_ricetta = """SELECT * 
  FROM [MTS].[Origine].[vRicette]
  where CdRecipe=? and DRecipe = ? and dat_obsoleto_ingredient is null"""
query_mts_ing=""""""
query_capecchi_ricetta = """SELECT *
  FROM [Ricette].[Application].[ItemsDetails]
  where CDrecipe=? and Cd_Ar=? and dat_obsoleto_ingredient is null"""
query_capecchu_ing=""""""

def modificheInsert(cdRicetta,stato,messaggio):
    print(cdRicetta,stato,messaggio)



print("inizio controllo modifiche ricette")

#download lista di tutte le ricette nel nostro sistema

query="""select * from ricette.application.items where Gestione='A'"""
ricetteCapecchi=DB.ReadData(query,())
print("Ricette Scaricate")

#downloaddi tutte le ricette da mts
query2="""SELECT distinct CdRecipe FROM [MTS].[Origine].[vRicette] where DtRecipeObsoleta is null"""
ricetteMTS=DB.ReadData(query2,())
print("MTS Scaricate")


for rc in ricetteCapecchi:
    array_cdCapecchi.append(rc[1])

for rc in ricetteMTS:
    array_cdMTS.append(rc[0])

for rc in ricetteCapecchi:
    if rc[1] not in array_cdMTS:
        array_obsoleteRicette.append(rc[1])
        modificheInsert(rc[0], "O", "Ricetta diventata obsoleta")

for rc in ricetteMTS:
    if rc[0] not in array_cdCapecchi:
        array_nuoveRicette.append(rc[0])
        modificheInsert(rc[0],"N","Nuova")
print("Ricette Nuove    ", len(array_nuoveRicette),array_nuoveRicette)
print("Ricette Obsolete ", len(array_obsoleteRicette),array_obsoleteRicette)

for rc in ricetteCapecchi:
    if rc[1] not in array_obsoleteRicette:
        if rc[1] not in array_nuoveRicette:
            messaggio=""
            modifiche=False
            ricettaMTS=DB.ReadData(query_mts_ricetta,(rc[1],rc[2]))
            ingredientiCapecchi=DB.ReadData(query_capecchi_ricetta,(rc[1],rc[2]))
            if len(ricettaMTS)>len(ingredientiCapecchi):
                messaggio=messaggio+"sono stati aggiunti uno o piu ingredienti"
                modifiche = True
            if len(ricettaMTS)<len(ingredientiCapecchi):
                messaggio = messaggio + "sono stati rimossi uno o piu elementi"
                modifiche = True
            if len(ricettaMTS)==len(ingredientiCapecchi):
                query_capecchi_ingredienti="""
                SELECT *
                FROM [Ricette].[Application].[ItemsDetails]
                where CDrecipe=? and Cd_Ar=? and versione = ?"""
                ingredientiCapecchi=DB.ReadData(query_capecchi_ingredienti,(rc[1],rc[2],rc[13]))
                for ing in ingredientiCapecchi:
                    if ing[4] != "IMBALLAGGI":
                        query_capecchi_schedeTecniche="""SELECT Revisione,Obsoleto
                        FROM [SchedeTecniche].[Schede].[Items]
                    	where id in (select max(id) FROM [SchedeTecniche].[Schede].[Items] where articolo=? and fornitore = ?)"""
                        Revisione=DB.ReadData(query_capecchi_schedeTecniche,(ing[6],ing[8]))
                        if Revisione is not None:

                            if Revisione[0][0]:
                                if ing[20] is None:
                                    if ing[24] == Revisione[0][0]:
                                        if Revisione[0][1]==1:
                                            messaggio = messaggio + F"{ing[6]}:Ingrediente diventato Obsoleto \n"
                                            modifiche = True

                                    else:
                                        messaggio = messaggio + F"{ing[6]}: Ingrediente modificato \n"
                                        modifiche = True

            if modifiche == True:
                print(f"RICETTA: {rc[1]}")
                print(messaggio)












#fine del processo di gestione delle modifiche


print("Processo terminato")


