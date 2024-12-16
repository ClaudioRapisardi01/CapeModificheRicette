import datetime
from time import sleep

import DB

# Declare

array_cdCapecchi = []  # array per la gestione dei codici ricetta presenti nel db Ricette dell app CapeApp
array_cdMTS = []  # array per la gestione dei codici ricetta presenti nel db di MTS
array_nuoveRicette = []  # array per la gestione delle nuove ricette
array_obsoleteRicette = []  # array per la gestione delle ricette diventate obsolete
query_mts_ricetta = """SELECT * 
  FROM [MTS].[Origine].[vRicette]
  where CdRecipe=? and DRecipe = ? and dat_obsoleto_ingredient is null"""
query_mts_ing = """"""
query_capecchi_ricetta = """SELECT *
  FROM [Ricette].[Application].[ItemsDetails]
  where CDrecipe=? and Cd_Ar=? and dat_obsoleto_ingredient is null"""
query_capecchu_ing = """"""
query_insert_lingueIng = """
 insert into Ricette.Application.LingueIngredienti
select distinct 
		?
		, ?
		, ?
		, ?
		, '' as Ingredienti_IT 
		, '' as Ingredienti_EN
		, '' as Ingredienti_FR
		, '' as Ingredienti_DE
		, '' as Ingredienti_ES
		, 'Latte|off,Cereali|off,Crostacei|off,Uova|off,Pesce|off,Arachidi|off,Soia|off,Frutta a guscio|off,Sedano|off,Senape|off,Sesamo|off,Solfiti|off,Lupini|off,Molluschi|off' as Allergeni
		, 'Latte|off,Cereali|off,Crostacei|off,Uova|off,Pesce|off,Arachidi|off,Soia|off,Frutta a guscio|off,Sedano|off,Senape|off,Sesamo|off,Solfiti|off,Lupini|off,Molluschi|off' as AllergeniFinali
		, '' as Nutrizionali_IT
		, '' as Nutrizionali_EN
		, '' as Nutrizionali_FR
		, '' as Nutrizionali_DE
		, '' as Nutrizionali_ES
		, '' as Nutrizionali_Porz_IT
		, '' as Nutrizionali_Porz_EN
		, '' as Nutrizionali_Porz_FR
		, '' as Nutrizionali_Porz_DE
		, '' as Nutrizionali_Porz_ES
		,'' as TestoAllergeni"""


def modificheInsert(cdRicetta, stato, messaggio):
    print(cdRicetta, stato, messaggio)
    query_insert = """
    INSERT INTO Ricette.[Application].[Items]
           (
           [CdRecipe]
           ,[Cd_Ar]
           ,[CdRecipeGroup]
           ,[DRecipeGroup]
           ,[DtRecipeObsoleta]
           ,[CdListRif]
           ,[CdArt]
           ,[DArt]
           ,[ArtUm]
           ,[QtaRecipe]
           ,[Estrazione]
           ,[locked]
           ,[Revisione]
           ,[Gestione]
           ,[NotaGestione])
     VALUES
           
           (?,
           ?,
           ?,
           ?,
           ?,
           ?,
           ?,
           ?,
           ?,
           ?,
           ?,
           ?,
           ?,
           ?,
           ?)"""
    query_get_ricetta = """
    select distinct [CdRecipe]
           ,[CdArt]
           ,[CdRecipeGroup]
           ,[DRecipeGroup]
           ,[DtRecipeObsoleta]
           ,[CdListRif]
           ,[CdArt]
           ,[DArt]
           ,[ArtUm]
           ,[QtaRecipe] from MTS.Origine.vRicette where CDrecipe=?"""
    query_get_ingredienti = """
    select [CDrecipe], CdArt, [NFase], [DFase], [PrgIngOrder], [CdIng], [DIng], st.[Fornitore], cf.[DCF], [RecipeUM], [IngUm], [RecipeIngUM], [QtaIng], [PCaloPesoIng], [qta_ingredient_tot_net], [prz_ingredient_um], [IngMltUMKG], [PrzIngKG], [flg_weight_enable], [dat_obsoleto_ingredient], [FAS], GETDATE(), 1, [Revisione]
    from MTS.Origine.vRicette
    left join SchedeTecniche.Schede.Items st on st.Articolo=CdIng and st.Preferenziale=1
    left join Arca.PowerBI.ClientiFornitori as cf on cf.CdCF=st.Fornitore
    where CdRecipe=?
    """
    query_get_max_versione = """
    select max(Revisione) 
    from Ricette.Application.Items 
    where CdRecipe=?"""
    query_insert_ingredienti = """
    INSERT INTO ricette.[Application].[ItemsDetails]
           (
           [CDrecipe]
           ,[Cd_Ar]
           ,[NFase]
           ,[DFase]
           ,[PrgIngOrder]
           ,[CdIng]
           ,[DIng]
           ,[Fornitore]
           ,[dcf]
           ,[RecipeUM]
           ,[IngUm]
           ,[RecipeIngUM]
           ,[QtaIng]
           ,[PCaloPesoIng]
           ,[qta_ingredient_tot_net]
           ,[prz_ingredient_um]
           ,[IngMltUMKG]
           ,[PrzIngKG]
           ,[flg_weight_enable]
           ,[dat_obsoleto_ingredient]
           ,[FAS]
           ,[Estrazione]
           ,[Versione]
           ,[Revisione])
     VALUES
          (
           ?,
           ?,
           ?,
           ?,
           ?,
           ?,
           ?,
           ?,
           ?,
           ?,
           ?,
           ?,
           ?,
           ?,
           ?,
           ?,
           ?,
           ?,
           ?,
           ?,
           ?,
           ?,
           ?,
           ?)"""
    if controllaPending(cdRicetta) == False:
        if stato == "N":
            Ricetta = DB.ReadData(query_get_ricetta, (cdRicetta))
            print(f"inserimento ricetta {Ricetta[0][0]}")
            print(Ricetta[0])
            ingredienti = DB.ReadData(query_get_ingredienti, (cdRicetta))
            DB.Execute(query_insert, (
            Ricetta[0][0], Ricetta[0][1], Ricetta[0][2], Ricetta[0][3], Ricetta[0][4], Ricetta[0][5], Ricetta[0][6],
            Ricetta[0][7], Ricetta[0][8], Ricetta[0][9], datetime.date.today(), 1, 1, stato,
            "Nuova ricetta inserita nel sistema"))
            DB.Execute(query_insert_lingueIng, (Ricetta[0][1], Ricetta[0][0], 1, datetime.date.today()))
            for i in ingredienti:
                print(i[0],
                      i[1],
                      i[2],
                      i[3],
                      i[4],
                      i[5],
                      i[6],
                      i[7],
                      i[8],
                      i[9],
                      i[10],
                      i[11],
                      i[12],
                      i[13],
                      i[14],
                      i[15],
                      i[16],
                      i[17],
                      i[18],
                      i[19],
                      i[20],
                      datetime.date.today(),
                      1,
                      i[23])
                DB.Execute(query_insert_ingredienti, (
                    i[0],
                    i[1],
                    i[2],
                    i[3],
                    i[4],
                    i[5],
                    i[6],
                    i[7],
                    i[8],
                    i[9],
                    i[10],
                    i[11],
                    i[12],
                    i[13],
                    i[14],
                    i[15],
                    i[16],
                    i[17],
                    i[18],
                    i[19],
                    i[20],
                    datetime.date.today(),
                    1,
                    i[23]

                ))
        else:

            Revisione = DB.ReadData(query_get_max_versione, (cdRicetta))
            RevisioneNuova = int(Revisione[0][0]) + 1
            Ricetta = DB.ReadData(query_get_ricetta, (cdRicetta))
            print(f"inserimento ricetta {Ricetta[0][0]}")
            DB.Execute(query_insert, (
                Ricetta[0][0], Ricetta[0][1], Ricetta[0][2], Ricetta[0][3], Ricetta[0][4], Ricetta[0][5], Ricetta[0][6],
                Ricetta[0][7], Ricetta[0][8], Ricetta[0][9], datetime.date.today(), 1, RevisioneNuova, stato,
                "Ricetta diventata Obsoleta nel sistema"))
            DB.Execute(query_insert_lingueIng, (Ricetta[0][1], Ricetta[0][0], RevisioneNuova, datetime.date.today()))
            ingredienti = DB.ReadData(query_get_ingredienti, (cdRicetta))
            for i in ingredienti:
                print(i[0],
                      i[1],
                      i[2],
                      i[3],
                      i[4],
                      i[5],
                      i[6],
                      i[7],
                      i[8],
                      i[9],
                      i[10],
                      i[11],
                      i[12],
                      i[13],
                      i[14],
                      i[15],
                      i[16],
                      i[17],
                      i[18],
                      i[19],
                      i[20],
                      datetime.date.today(),
                      RevisioneNuova,
                      i[23])
                DB.Execute(query_insert_ingredienti, (
                    i[0],
                    i[1],
                    i[2],
                    i[3],
                    i[4],
                    i[5],
                    i[6],
                    i[7],
                    i[8],
                    i[9],
                    i[10],
                    i[11],
                    i[12],
                    i[13],
                    i[14],
                    i[15],
                    i[16],
                    i[17],
                    i[18],
                    i[19],
                    i[20],
                    datetime.date.today(),
                    RevisioneNuova,
                    i[23]))

    else:
        print(f"Ricetta {cdRicetta}:Gia presente un altra modifica da gestire")


def controllaPending(cdRecipe):
    query_controlla_pending = """
        select * from Ricette.application.items where cdrecipe=?
        """
    Ricetta = DB.ReadData(query_controlla_pending, (cdRecipe))
    risposta = False
    for r in Ricetta:
        if r[14] != 'A' and r[14] != 'G':
            return True

    return risposta


print("inizio controllo modifiche ricette")

# download lista di tutte le ricette nel nostro sistema

query = """select * from ricette.application.items where Gestione='A'"""
ricetteCapecchi = DB.ReadData(query, ())
print("Ricette Scaricate")

# downloaddi tutte le ricette da mts
query2 = """SELECT distinct CdRecipe FROM [MTS].[Origine].[vRicette] where DtRecipeObsoleta is null """
ricetteMTS = DB.ReadData(query2, ())
print("MTS Scaricate")

for rc in ricetteCapecchi:
    array_cdCapecchi.append(rc[1])

for rc in ricetteMTS:
    array_cdMTS.append(rc[0])

for rc in ricetteCapecchi:
    if rc[1] not in array_cdMTS:
        array_obsoleteRicette.append(rc[1])
        modificheInsert(rc[1], "O", "Ricetta diventata obsoleta")

for rc in ricetteMTS:
    if rc[0] not in array_cdCapecchi:
        array_nuoveRicette.append(rc[0])
        modificheInsert(rc[0], "N", "Nuova")

for rc in ricetteCapecchi:
    if rc[1] not in array_obsoleteRicette:
        if rc[1] not in array_nuoveRicette:
            messaggio = ""
            modifiche = False
            ricettaMTS = DB.ReadData(query_mts_ricetta, (rc[1], rc[2]))
            ingredientiCapecchi = DB.ReadData(query_capecchi_ricetta, (rc[1], rc[2]))
            if len(ricettaMTS) > len(ingredientiCapecchi):
                messaggio = messaggio + "sono stati aggiunti uno o piu ingredienti"
                # modifiche = True
            if len(ricettaMTS) < len(ingredientiCapecchi):
                messaggio = messaggio + "sono stati rimossi uno o piu elementi"
                # modifiche = True
            if len(ricettaMTS) == len(ingredientiCapecchi):
                query_capecchi_ingredienti = """
                SELECT *
                FROM [Ricette].[Application].[ItemsDetails]
                where CDrecipe=? and Cd_Ar=? and versione = ?"""
                ingredientiCapecchi = DB.ReadData(query_capecchi_ingredienti, (rc[1], rc[2], rc[13]))
                for ing in ingredientiCapecchi:
                    query_obs = """select * from Ricette.dbo.IngredienteObsoletoRicetta(?)"""
                    obsoleto = DB.ReadData(query_obs, (ing[6]))
                    if obsoleto[0][2] == 0:
                        query_modifica_ing = """
                        SELECT distinct ir.CdIng, 
                        CASE
                        WHEN Ir.Revisione != s.Revisione THEN 1
                        WHEN Ir.Revisione = s.Revisione THEN 0
                        ELSE 0
        
                        END AS obs
                        FROM Ricette.Application.ItemsDetails as IR
                        left join (select Articolo,Revisione from SchedeTecniche.Schede.Items where id in (select max(id) from SchedeTecniche.Schede.Items where Articolo=? and Preferenziale=1)) as s on s.Articolo= IR.CdIng
                        where ir.CdIng=?
                        """
                        modifica=DB.ReadData(query_modifica_ing,(ing[6],ing[6]))
                        if modifica[0][1]==0:
                            print(f"ING {ing[6]}:ok")
                        else:
                            messaggio = messaggio + f"Nuova Revisione per ingrediente {ing[6]}"
                            modifiche = True
                    else:
                        messaggio = messaggio + f"\ningrediente {ing[6]} diventato obsoleto"
                        modifiche = True

            if modifiche == True:
                print(rc)
                print(f"RICETTA: {rc[1]}")
                modificheInsert(rc[1], "M", messaggio)

# fine del processo di gestione delle modifiche


print("Processo terminato")
