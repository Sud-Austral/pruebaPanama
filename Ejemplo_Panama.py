#import git
import pandas as pd
import datetime
import http.client, urllib.request, urllib.parse, urllib.error, base64
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import tweepy
from azure.ai.textanalytics import TextAnalyticsClient
from azure.core.credentials import AzureKeyCredential
import requests
import json  
import codecs
import numpy as np
import wget
import http.client, urllib.request, urllib.parse, urllib.error, base64
import smtplib
from email.mime.text import MIMEText
import time
#import R0

#************************************Ciclo general********************************************************
#************************************Ciclo general********************************************************
horarios = [
            "11:30",
            "13:30",
            "16:30",
            "18:30",
            "20:30",
            "22:00",
            "23:30"
            ]

def Ciclo():
    while True:
        now = datetime.datetime.now() - datetime.timedelta(hours = 4)
        hora = now.strftime("%H:%M")
        #print(hora)
        for i in horarios:
            if(i == hora):
                print("***************************             " + hora + "           *****************************")
                UpdateDatabase()
                time.sleep(3600)
    #if(hora == "23:30"):
    #    print("***************************             " + hora + "           *****************************")
        #UpdateDatabase()
    #    time.sleep(43200)
    #if(hora == "20"):
    #    UpdateDatabase()    
        time.sleep(55)
    #Ciclo()            

#************************************Ciclo general********************************************************
#************************************Ciclo general********************************************************

#************************************Bucle general********************************************************
def UpdateDatabase():
    print("Comenzo...")
    try:
        datasetFinalTweet()
        print("Los tweet se cargaron correctamente...")
    except:
        print("Error al cargar los Tweet")
    try:
        bingNews()
        print("Las noticias se cargaron correctamente...")
    except:
        print("Error al cargar los Noticias")
    try:
        guardarDataCovid()
        print("Los datos de datacovid se cargaron correctamente...")
    except:
        print("Error al cargar los archivos del la organizacion")
    try:
        SaveArgis()
        print("Los datos de ARGIS se cargaron correctamente...")
    except:
        print("Error al cargar los archivos del la ARGIS")
    guardarRepositorio()
    print("Ahora a esperar el R0, empezando a las " + datetime.datetime.now().strftime("%H:%M:%S"))    
    #try:
    #    R0.funcionglobal()
    #except:
    #    print("Error al cargar los archivos del la ARGIS") 
    print("Y termino a las " + datetime.datetime.now().strftime("%H:%M:%S"))       
    guardarRepositorio()
    return

#************************************Bucle general********************************************************
#************************************Actualizar repositorio***********************************************
def guardarRepositorio():
    #Esta linea crea un objeto para manejar el repositorio alojado en la ruta
    #Correspondinete al argumento entregado en String
    repoLocal = git.Repo('C:/Users/datos/Documents/GitHub/Datos_Panama')  
    try:
        #Agrego todos los archivos nuevos
        repoLocal.git.add(".")
        #Hace el commit con un comentario sobre el origen del mismo y la hora
        repoLocal.git.commit(m='Update automatico via Actualizar ' + datetime.datetime.now().strftime("%m-%d-%Y %H-%M-%S"))
        #Apuntamos al gitHub (online)
        origin = repoLocal.remote(name='origin')
        #Se hace el push (enviar los archivos al repositorio online)
        origin.push()
        #Mensaje simpatico que todo salio bien
        print("Repositorio actualizado =)")
    except:
        #Da un mensaje de error al fallar
        print("Error de GITHUB")    
    return

#************************************Actualizar repositorio***********************************************
#************************************Analisis Twitter*******************************************
def getKeys():
    f = open('C://key.json','r')
    keys = f.read()
    jkeys = json.loads(keys)
    return jkeys

    #************************************Analisis Twitter*******************************************

#************************************Actualizar Datos Twiter*******************************************
#Esta funcion devuelve la api necesaria para hacer todas las consultas requeridas
def APITWEET():
    # Declaramos nuestras Twitter API Keys:
    keys = getKeys()
    #ACCESS_TOKEN = '1230251564616515586-2KqPsCG2mIJp3irRjENgHpCfQUxTUg'
    #ACCESS_TOKEN_SECRET = '6PJfMtYGY7w6csiIX9m1S5jFEKNZ3hE9PVkHKeN1S14iM'
    #CONSUMER_KEY = 'koO4XqTuWFr5ADGcE8kjIkVoU'
    #CONSUMER_SECRET = '3F4sk9qU8zbKBROuLPUUj1uvE2YuhseXPe0ahMQoivg4icN5bL'  
    ACCESS_TOKEN = keys['twitter']['token_acceso']
    ACCESS_TOKEN_SECRET = keys['twitter']['secreto_token_acceso']
    CONSUMER_KEY = keys['twitter']['clave_api']
    CONSUMER_SECRET = keys['twitter']['clave_secreta_api']
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    api = tweepy.API(auth)
    return api
#Tomar la fecha que viene en los tweet en formato cadena de texto y transformarla en formato datetime
def FechaTweeter(palabra):
    anio = int(palabra[-4:])
    meses = {
        "Jan":1,
        "Feb":2,
        "Mar":3,
        "Apr":4,
        "May":5,
        "Jun":6,
        "Jul":7,
        "Aug":8,
        "Sep":9,
        "Oct":10,
        "Nov":11,
        "Dec":12
    }
    mes = meses[palabra[4:7]]
    dia = int(palabra[8:10])
    hora = int(palabra[11:13]) 
    minuto = int(palabra[14:16])
    segundo = int(palabra[17:19])
    return datetime.datetime(anio,mes,dia,hora,minuto,segundo) - datetime.timedelta(hours = 6)
#Limpiar el campo source
def depurarFuenteTweet(palabra):
    salida = palabra.replace('<a href="https://about.twitter.com/products/tweetdeck" rel="nofollow">','').replace("</a>","")
    salida = salida.replace('<a href="http://twitter.com/download/iphone" rel="nofollow">',"")
    salida = salida.replace('<a href="https://studio.twitter.com" rel="nofollow">',"")
    salida = salida.replace('<a href="https://mobile.twitter.com" rel="nofollow">',"")
    salida = salida.replace('<a href="http://twitter.com" rel="nofollow">',"")
    salida = salida.replace('<a href="http://twitter.com/download/android" rel="nofollow">',"")
    salida = salida.replace('<a href="https://www.hootsuite.com" rel="nofollow">',"")
    #salida = salida.replace('<a href=""http://twitter.com/download/android"" rel=""nofollow"">',"")
    return salida
#A partir del usuario user se devuelve en una lista los ultimos 10 tweet
def get_tweetConFecha(user, api = APITWEET()):
    return list(api.user_timeline(screen_name = user, count= 10))

#Crea un dataset con los ultimos 10 tweet
def definirDatasetPorCuenta(cuenta):
#lista = get_tweetConFecha("colmedchile")
    lista = get_tweetConFecha(cuenta)
    salida = []
    for i in lista:  
        jsonObject = i._json.copy()
        datos = {
                    "Contenido" : jsonObject["text"], 
                    "IR" : "https://twitter.com/i/web/status/" + jsonObject["id_str"], 
                    "Fecha" : FechaTweeter(jsonObject["created_at"]).strftime("%d/%m/%Y %H:%M:%S"),
                    "Dispositivo" : depurarFuenteTweet(jsonObject["source"]),
                    "Likes" : jsonObject["favorite_count"],
                    "Retweets" : jsonObject["retweet_count"],
                    "Entidad" : jsonObject["user"]["name"],
                    "Hora" : FechaTweeter(jsonObject["created_at"]).strftime("%H:%M:%S"),
                    "Foto": jsonObject["user"]["profile_image_url"].replace("_normal.","."),
                    "FechaAux": FechaTweeter(jsonObject["created_at"])
                }
        salida.append(datos.copy())
    data = pd.DataFrame(salida)
    return data
#Crea un dataset con todas las cuentas en la lista cuentas
def datasetFinalTweet():
    cuentas = [
                "MINSAPma",
                "opsoms"
                ]
    salida = []
    for i in cuentas:
        salida.append(definirDatasetPorCuenta(i))
    data = pd.concat(salida)
    data = data.sort_values(by=['FechaAux'])
    del data["FechaAux"]
    data.to_csv("../Datos_Panama/tweeter/tweeter.csv", index=False)
    return data

#tweepy.Cursor(api.search, q='#मराठी OR #माझाक्लिक OR #म')
#tweepy.Cursor(api.friends)
#tweepy.Cursor(api.home_timeline)
#tweepy.Cursor(api.search, url)
#tweepy.Cursor(api.friends, user_id=user_id, count=200).items()
#tweepy.Cursor(api.mentions_timeline, user_id=user_id, count=200).items()
#######https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/tweet-object

#************************************Actualizar Datos Twiter*******************************************
#************************************Actualizar BING NEWS*******************************************
"""
Argentina	AR
Australia	AU
Austria	AT
Belgium	BE
Brazil	BR
Canada	CA
Chile	CL
Denmark	DK
Finland	FI
France	FR
Germany	DE
Hong Kong SAR	HK
India	IN
Indonesia	ID
Italy	IT
Japan	JP
Korea	KR
Malaysia	MY
Mexico	MX
Netherlands	NL
New Zealand	NZ
Norway	NO
China	CN
Poland	PL
Portugal	PT
Philippines	PH
Russia	RU
Saudi Arabia	SA
South Africa	ZA
Spain	ES
Sweden	SE
Switzerland	CH
Taiwan	TW
Turkey	TR
United Kingdom	GB
United States	US
"""
def fechaCorrecta(i):
    año = i[0:4]
    mes = i[5:7]
    dia = i[8:10]
    hora = i[11:13]
    minuto = i[14:16]
    return dia + "-" + mes + "-" + año + " " + hora + ":" + minuto

def reemplazarFinal(i):
    return i.replace("&pid=News","")

def bingNews(pais = "Panama"):
    #pais = "Chile"
    headers = {
        # Request headers
        #'Ocp-Apim-Subscription-Key': 'b091fbaeb9f94255b542befc3ecff8b8',
        'Ocp-Apim-Subscription-Key': 'a9b5b1527a7b43929d7e15a383b1583a',
    }

    params = urllib.parse.urlencode({
        # Request parameters
        'q':  'covid-19 coronavirus ' + pais + ' loc:pa FORM=HDRSC4',
        'count': '40',
        'offset': '0',
        'mkt': 'es-US',
        'safeSearch': 'Moderate',
        "sortBy": "Date"
    })

    #conn = http.client.HTTPSConnection('api.cognitive.microsoft.com')
    conn = http.client.HTTPSConnection('dataintelligence.cognitiveservices.azure.com')
    conn.request("GET", "/bing/v7.0/news/search?%s" % params, "{body}", headers)
    response = conn.getresponse()
    #data = response.read()

    decoded_data=codecs.decode(response.read(), 'utf-8-sig')
    d = json.loads(decoded_data)
    conn.close()
    aux =  d['value']
    salida = []
    for i in aux:
        try:
            i["imagen"] = i["image"]["thumbnail"]["contentUrl"]
            i["pais"] = "Panamá"
            try:
                i["Fuente"] = i['provider'][0]["name"]
            except:
                pass
            salida.append(i.copy())
        except:
            pass
    data = pd.DataFrame(salida)[["name","url","description","datePublished","imagen","pais","Fuente"]]
    data["datePublished"] = data["datePublished"].apply(fechaCorrecta)
    data["imagen"] = data["imagen"].apply(reemplazarFinal)
    data[::-1].to_csv("../Datos_Panama/bing/News/Panama.csv",index=False)
    return

     #************************************Actualizar BING NEWS*******************************************
#************************************Actualizar Datos de la organizacion*******************************************
def guardarDataCovid():
    #00 DATACOVID Trabajo_PN.xlsx
    url = "https://onedrive.live.com/download?cid=9f999e057ad8c646&page=view&resid=9F999E057AD8C646!63087&parId=9F999E057AD8C646!63073&authkey=!AkePW7UW1KXQkMM&app=Excel"
    urllib.request.urlretrieve(url, "../Datos_Panama/datacovidpa/00 DATACOVID Trabajo_PN.xlsx")
    #urllib.request.urlretrieve(url, "../Datos_Honduras/datacovidhn/Covid HN.xlsx")

    #00 DATACOVID_PN_CUARENTENA.xlsx
    url = "https://onedrive.live.com/download?cid=9f999e057ad8c646&page=view&resid=9F999E057AD8C646!63091&parId=9F999E057AD8C646!63073&authkey=!AkePW7UW1KXQkMM&app=Excel"
    urllib.request.urlretrieve(url, "../Datos_Panama/datacovidpa/00 DATACOVID_PN_CUARENTENA.xlsx")
    #Alimentacion PN.xlsx
    url = "https://onedrive.live.com/download?cid=9f999e057ad8c646&page=view&resid=9F999E057AD8C646!63076&parId=9F999E057AD8C646!63073&authkey=!AkePW7UW1KXQkMM&app=Excel"
    urllib.request.urlretrieve(url, "../Datos_Panama/datacovidpa/Alimentación PN.xlsx")
    #Casos PN Diarios.xlsx
    url = "https://onedrive.live.com/download?cid=9f999e057ad8c646&page=view&resid=9F999E057AD8C646!63109&parId=9F999E057AD8C646!63073&authkey=!AkePW7UW1KXQkMM&app=Excel"
    urllib.request.urlretrieve(url, "../Datos_Panama/datacovidpa/Casos PN Diarios.xlsx")
    #ECONOMICOS PN.xlsx
    url = "https://onedrive.live.com/download?cid=9f999e057ad8c646&page=view&resid=9F999E057AD8C646!63088&parId=9F999E057AD8C646!63073&authkey=!AkePW7UW1KXQkMM&app=Excel"
    urllib.request.urlretrieve(url, "../Datos_Panama/datacovidpa/ECONOMICOS PN.xlsx")
    #Condición_Pacientes PN.xlsx
    url = "https://onedrive.live.com/download?cid=9f999e057ad8c646&page=view&resid=9F999E057AD8C646!63085&parId=9F999E057AD8C646!63073&authkey=!AkePW7UW1KXQkMM&app=Excel"
    urllib.request.urlretrieve(url, "../Datos_Panama/datacovidpa/Condición_Pacientes PN.xlsx")
    #Farmacias PN.xlsx
    url = "https://onedrive.live.com/download?cid=9f999e057ad8c646&page=view&resid=9F999E057AD8C646!63078&parId=9F999E057AD8C646!63073&authkey=!AkePW7UW1KXQkMM&app=Excel"
    urllib.request.urlretrieve(url, "../Datos_Panama/datacovidpa/Farmacias PN.xlsx")
    #Localiza PN v1.xlsx
    url = "https://onedrive.live.com/download?cid=9f999e057ad8c646&page=view&resid=9F999E057AD8C646!63107&parId=9F999E057AD8C646!63073&authkey=!AkePW7UW1KXQkMM&app=Excel"
    urllib.request.urlretrieve(url, "../Datos_Panama/datacovidpa/Localiza PN v1.xlsx")
    #Salud PN.xlsx
    url = "https://onedrive.live.com/download?cid=9f999e057ad8c646&page=view&resid=9F999E057AD8C646!63080&parId=9F999E057AD8C646!63073&authkey=!AkePW7UW1KXQkMM&app=Excel"
    urllib.request.urlretrieve(url, "../Datos_Panama/datacovidpa/Salud PN.xlsx")

    url = "https://onedrive.live.com/download?cid=9f999e057ad8c646&page=view&resid=9F999E057AD8C646!63744&parId=9F999E057AD8C646!63073&authkey=!AkePW7UW1KXQkMM&app=Excel"
    urllib.request.urlretrieve(url, "../Datos_Panama/datacovidpa/Pagina_Inicio_PN.xlsx")
    return 

#************************************Actualizar Datos de la organizacion*******************************************

#************************************Actualizar ARGIS*******************************************
"""
DEPRECATED
def GuardarDatos(api):
    ruta = "../Datos_Panama/ARGIS/"
    #response = requests.get("https://opendata.arcgis.com/datasets/e1ae054ba64342dcb4a3b892d83e0f75_0.geojson")
    response = requests.get(api[0])
    decoded_data=codecs.decode(response.content, 'utf-8-sig')
    d = json.loads(decoded_data)
    salida = []
    for i in d["features"]:
        salida.append(i['properties'].copy())
    pd.DataFrame(salida).to_csv(ruta + api[1] + ".csv", index="False")
    return 

def SaveArgis():
    apis = [
    ("https://opendata.arcgis.com/datasets/b72742fe3391463ea51cd7196a7f5e8c_0.geojson", "HISTORICO_PRUEBAS"),
    ("https://opendata.arcgis.com/datasets/e1ae054ba64342dcb4a3b892d83e0f75_0.geojson", "CASOS_ACUMULATIVOS"),
    ("https://opendata.arcgis.com/datasets/3063bbd564494682aad473f794684f61_0.geojson", "PRUEBAS"),
    ("https://opendata.arcgis.com/datasets/4cc2d9431ecf406aa1ab039fed1de668_0.geojson", "CORREGIMIENTOS")
    ]
    for api in apis:
        GuardarDatos(api)
    return
"""
def GuardarDatos(api):
    ruta = "../Datos_Panama/ARGIS/"
    #response = requests.get("https://opendata.arcgis.com/datasets/e1ae054ba64342dcb4a3b892d83e0f75_0.geojson")
    response = requests.get(api[0])
    decoded_data=codecs.decode(response.content, 'utf-8-sig')
    d = json.loads(decoded_data)
    salida = []
    for i in d["features"]:
        salida.append(i['properties'].copy())
    pd.DataFrame(salida).to_csv(ruta + api[1] + ".csv", index="False")
    return 

def SaveArgis():
    apis = [
    ("https://opendata.arcgis.com/datasets/b72742fe3391463ea51cd7196a7f5e8c_0.geojson", "HISTORICO_PRUEBAS"),
    ("https://opendata.arcgis.com/datasets/e1ae054ba64342dcb4a3b892d83e0f75_0.geojson", "CASOS_ACUMULATIVOS"),
    ("https://opendata.arcgis.com/datasets/3063bbd564494682aad473f794684f61_0.geojson", "PRUEBAS"),
    ("https://opendata.arcgis.com/datasets/4cc2d9431ecf406aa1ab039fed1de668_0.geojson", "CORREGIMIENTOS")
    ]
    enviar = False
    bodyCorreo = ''
    for api in apis:
        GuardarDatos(api)
        resultado = definirCorreo(api[1])
        bodyCorreo = bodyCorreo + resultado[0]
        enviar = (enviar | resultado[1])
    print(enviar)
    if(enviar):
        enviarCorreo(bodyCorreo)
    return

def enviarCorreo(mensaje):
    print(mensaje)
    keys = getKeys()
    #from_addr = 'informaticasudaustral@gmail.com'
    from_addr = keys['correo']['username']
    #keys['correo']['username']
    #correo receptor
    #to = 'lmonsalve22@gmail.com'
    to = [
        'nataliaarancibiap@gmail.com',
        'lmonsalve22@gmail.com',
        'carolina.mario.c@gmail.com',
        'paularojasantander@gmail.com',
        'fernanda.olivares.nu2995@gmail.com'
    ]
    mensaje = mensaje + "\n"
    mensaje = mensaje + 'https://github.com/Sud-Austral/Datos_Panama/tree/master/ARGIS'
    #mensaje que se envia por el correo
    #message = MIMEText('----PROBANDO CORREO POR PYTHON----')
    message = MIMEText(mensaje)

    #asunto del correo
    message['Subject'] = 'Actualizacion Github Panama'
    message['From'] = keys['correo']['username']
    message['To'] = 'lmonsalve22@gmail.com'

    # credenciales del correo emisor
    username = keys['correo']['username']
    password = keys['correo']['password']

    server = smtplib.SMTP('smtp.gmail.com:587')
    server.starttls()
    server.login(username, password)
    server.sendmail(from_addr, to, message.as_string())

    server.quit()
    return

def definirCorreo(nombre):
    enviar = False
    ruta = "../Datos_Panama/ARGIS/"
    rutaGit = "https://raw.githubusercontent.com/Sud-Austral/Datos_Panama/master/ARGIS/"
    nombreArchivo = nombre + ".csv"
    dataNuevo = pd.read_csv(ruta + nombreArchivo)
    #dataNuevo = pd.read_csv(rutaGit + nombreArchivo)
    dataViejo = pd.read_csv(rutaGit + nombreArchivo)
    salida = ""
    try:
        fechaNuevo = dataNuevo["FECHA"][len(dataNuevo["FECHA"]) - 1]
        fechaViejo = dataViejo["FECHA"][len(dataViejo["FECHA"]) - 1]
        if(fechaNuevo != fechaViejo):
            enviar = True            
            hora = (datetime.datetime.now() - datetime.timedelta(hours=4)).strftime("a las %H-%M-%S en día %m-%d-%Y ")
            salida = "El archivo " + nombreArchivo + " fue actualizado " + hora
        else:
            salida = "El archivo " + nombreArchivo + " no fue actualizado"
    except:
        #fechaNuevo = dataNuevo.shape[0]
        #fechaViejo = dataViejo.shape[0]
        globalNuevo = dataNuevo["GlobalID"][0]
        globalViejo = dataViejo["GlobalID"][0]
        if(globalNuevo != globalViejo):
            enviar = True
            hora = (datetime.datetime.now() - datetime.timedelta(hours=4)).strftime("a las %H-%M-%S en día %m-%d-%Y ")
            salida = "El archivo " + nombreArchivo + " fue actualizado " + hora
        else:
            salida = "El archivo " + nombreArchivo + " no fue actualizado"
    return salida + "\n", enviar
#************************************Actualizar ARGIS*******************************************

if __name__ == '__main__':
	UpdateDatabase()
#sp.Ciclo()