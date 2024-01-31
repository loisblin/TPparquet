from fonction import *
import pyarrow.compute as pc
df_ville=pd.read_csv('villes_virgule.csv')
df_academies=pd.read_csv('academies_virgule.csv')
#print(get_column(table_academie, "dep"))
#print(table_ville)
def filtrer_par_ville(dataset, ville):
    result = dataset[dataset['nom'] == ville]
    return result


def filtrer_par_departement_trier_par_ville(dataset, departement):
    filtered_data = dataset[dataset['dep'] == departement]
    sorted_data = filtered_data.sort_values(by='nom')
    return sorted_data
tf=filtrer_par_ville(df_ville,'Annecy')

tff=filtrer_par_departement_trier_par_ville(df_ville,'74')


def join(df1,df2,id):
    return(pd.merge(df1,df2, on=id))
df_join_ville_academies=join(df_ville,df_academies,'dep')

def display_ville_zone(df):

    result = df[['nom', 'vacances']]
    print(result)



def number_ville_accademie(df):
    result = df.groupby('academie')['nom'].count().reset_index()
    result.columns = ['academie', 'nombre_villes']
    print(result)
number_ville_accademie(df_join_ville_academies)