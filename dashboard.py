
import pandas as pd
import warnings
warnings.filterwarnings("ignore")
data_horas = pd.read_excel('ensayo.xlsx')
data_horas = data_horas.rename(columns={
    'Resumen de Organizadores': 'MeetingId',
    'Unnamed: 1': "Numero de participantes",
    'Unnamed: 2': "Empresa",
    'Unnamed: 3': "Email",
    "Unnamed: 4": "Nombre",
    "Unnamed: 5": "inicio asistencia",
    "Unnamed: 6": "fin asistencia",
    "Unnamed: 7": "id participante",
    "Unnamed: 8": "Rol",
    "Unnamed: 9": "hora de ingreso",
    "Unnamed: 10": "hora de salida",
    "Unnamed: 11": "segundos asistencia",
    "Unnamed: 12": "tiempo conectado",
    "Unnamed: 13": "tenant id",
    "Unnamed: 14": "tenant",
    "Unnamed: 15": "Match Meeting Id",
    "Unnamed: 16": "duracion planeada",
    "Unnamed: 17": "inicio agendado",
    })
#eliminamos la primera fila, ya que contiene informacion del excel original, que aqui no es relevante
data_horas=data_horas.drop([0],axis=0)
#seleccionamos las columnas mas relevantes para el analisis
filt=data_horas[["Nombre","Numero de participantes","Email","Rol","Empresa","hora de ingreso","duracion planeada","inicio agendado","tiempo conectado"]]
filt=filt.drop_duplicates().reset_index(drop=True)
filt.index=filt.index+1
#En esta parte arreglamos un pequeño error en los emails que se nos presenta 
# para evitar errores futuros en el procesamiento de las columnas

for i in filt.index:  
    if isinstance(filt.loc[i, "Email"], float):
        filt.loc[i, "Email"] = "None"

import re
patron  = r"analista[a-zA-Z0-9_.+-]*@talentoconsultores"
#utilizamos esta funcion auxiliar para separar los analistas
def aux_fun_error(email):
    if re.search(patron, email, re.IGNORECASE):
        return 1
    else:
        return 0
#creamos una nueva columna temporal asignando el valor en 
# binario de si es el caso de un analista o no
filt["Es_analista"]= filt.apply(lambda x:aux_fun_error(x["Email"]),axis=1)


#Vamos a arreglar un error que nos encontramos 
# y es que aveces a los analistas se les asigna un nombre erroneo. 
# Posiblemente de un asistente a la reunión y por tanto esto nos puede evitar 
# obtener información correcta de los analistas.

analistas= filt[filt["Es_analista"]==1]["Email"].unique()
dict_nombres={analista:[] for analista in analistas}
for i in range(filt.shape[0]):
    if filt.loc[i+1,"Es_analista"]==1:
        dict_nombres[filt.loc[i+1,"Email"]].append(filt.loc[i+1,"Nombre"])
from statistics import mode
llaves=list(dict_nombres.keys())
dict_nombres_moda={correo:mode(dict_nombres[correo]) for correo in llaves}
for i in range(filt.shape[0]):
    if filt.loc[i+1,"Es_analista"]==1:
        filt["Nombre"].iloc[i]=dict_nombres_moda[filt.loc[i+1,"Email"]]

 #Se arregla el error de que una persona se desconecta y se vuelta a conectar 
 # varias veces en la reunion. Lo cual dañaba los tiempos para el analisis 

from datetime import timedelta
from datetime import datetime as dt
#vamos a usar en cambio de formato inicial en varios columnas,
#  para tenerlas como fecha y asi poder hacer la manipulacion que queremos 
# hacer para que se le sumen los tiempos de conexión a la persona que pudo
#  haberse salido y vuelto a ingresar varias veces a la reunion.
filt["tiempo conectado"]=filt.apply(lambda x: pd.to_timedelta(x["tiempo conectado"]),axis=1)
filt["hora de ingreso"]=filt.apply(lambda x: pd.to_datetime(x["hora de ingreso"])-timedelta(hours=5) if str(x["hora de ingreso"])!="System.Object[]" else  dt(1990,1,1),axis=1)
filt["Nombre"]=filt.apply(lambda x: str(x["Nombre"]).strip(),axis=1)
for i in range(filt.shape[0]):
    if filt.loc[i+1,"Rol"]=="Organizer":
        lista_asistentes=[]
        numero_asistentes=filt.loc[i+1,"Numero de participantes"]
        for j in range(1,numero_asistentes):
            if filt.loc[i+j+1,"Nombre"] in lista_asistentes:
                #vamos a ubicar la suma de los tiempos de conexion en la primera aparicion del asistente
                x=j
                numero_cambio=lista_asistentes.index(filt.loc[i+j+1,"Nombre"])+1
                filt["tiempo conectado"].iloc[i+numero_cambio]=filt.loc[i+1+numero_cambio,"tiempo conectado"]+filt.loc[i+1+j,"tiempo conectado"]
                filt["hora de ingreso"].iloc[i+numero_cambio]=min(filt.loc[i+1+numero_cambio,"hora de ingreso"],filt.loc[i+1+j,"hora de ingreso"])
                lista_asistentes.append(filt.loc[i+j+1,"Nombre"])
            else:
                lista_asistentes.append(filt.loc[i+j+1,"Nombre"])




#creamos nuevas columnas para obtener posteriormente los tiempos muertos por reunion
filt["tiempos"]=filt.apply(lambda x: [], axis=1)
filt["tiempos inevitables"]=filt.apply(lambda x: [], axis=1)

#en este proceso vamos a almacenar los tiempos que duraron los asistentes
#  en la reunion junto con su hora de entrada
filt["tiempos"]=filt.apply(lambda x: [], axis=1)
filt["tiempos inevitables"]=filt.apply(lambda x: [], axis=1)

for i in range(filt.shape[0]):
    if filt.loc[i+1,"Rol"]=="Organizer":
        numero_asistentes=filt.loc[i+1,"Numero de participantes"]
        if numero_asistentes>1:
          lista=[]
          inevitables=[]
          for j in range(1,numero_asistentes):
             if filt.loc[i+j+1,"Es_analista"]==0:
                lista.append(filt.loc[i+j+1,"tiempo conectado"])
                inevitables.append(filt.loc[i+j+1,"hora de ingreso"])
          
          filt.at[i+1,"tiempos"]=lista
          filt.at[i+1,"tiempos inevitables"]=inevitables
#seleccionamos como un aproximado del tiempo que duraron los asistentes 
# a la persona que mas tiempo duro dentro de la reunion y seleccionamos
#  su hora de llegada a la reunion
filt["tiempo conectado asistentes"]=filt.apply(lambda x: max(x["tiempos"])if len(x["tiempos"])>0 else 0, axis=1)
filt["tiempo de entrada inevitable"]=filt.apply(lambda x:x["tiempos inevitables"][x["tiempos"].index(max(x["tiempos"]))]if len(x["tiempos"])>0 else x["hora de ingreso"],axis=1)
filt=filt.drop(columns=["Es_analista"])  
#seleccionamos unicamente a los organizadores de las reuniones, 
# ya que dentro de esto se van a encontrar todas las reuniones de los analistas
filt=filt[filt["Rol"]=="Organizer"]
filt=filt.drop(columns=["Rol"])


#Realizaremos un filtrado de los correos utilizando expresiones regulares 
# para obtener unicamente los correos de los analistas. 
# Los cuales siempre inician por analista
import re
patron  = r"analista[a-zA-Z0-9_.+-]*@talentoconsultores"
#utilizamos esta funcion auxiliar para separar los analistas
def aux_fun(email):
    if re.search(patron, email, re.IGNORECASE):
        return email
    else:
        return "None"
filt.Email=filt.apply(lambda x: aux_fun(x.Email),axis=1) 

#Dejamos unicamente los correos de los analistas
filt=filt[filt.Email!="None"]

#transformamos todas las fechas y tiempos a formato datetime o deltatime 
# segun corresponda, para hacer las manipulaciones temporales mas sencillas
from datetime import timedelta
from datetime import datetime as dt
filt=filt.drop(columns=["tiempos","tiempos inevitables"])
def fecha_ing(fecha):
    spanish_months = {
    'ene.': 'Jan', 'feb.': 'Feb', 'mar.': 'Mar', 'abr.': 'Apr',
    'may.': 'May', 'jun.': 'Jun', 'jul.': 'Jul', 'ago.': 'Aug',
    'sep.': 'Sep', 'oct.': 'Oct', 'nov.': 'Nov', 'dic.': 'Dec'
}
    for spa in spanish_months:
        if spa in fecha:
          fecha = fecha.replace(spa, spanish_months[spa])
        else:
            pass
    return fecha
filt["inicio agendado"]=filt.apply(lambda x: fecha_ing(x["inicio agendado"]),axis=1) 

filt["inicio agendado"]=filt.apply(lambda x: pd.to_datetime(x["inicio agendado"]),axis=1)
filt["duracion planeada"]=filt.apply(lambda x: pd.to_timedelta(x["duracion planeada"]),axis=1)

filt["tiempo conectado asistentes"]=filt.apply(lambda x: pd.to_timedelta(x["tiempo conectado asistentes"]),axis=1)


#Vamos a revisar que no se introduzcan reuniones que aún no han sucedido
#  hasta la recoleccion da datos.
year_actual,mes_actual,dia_actual=dt.now().year,dt.now().month,dt.now().day
fecha=dt(year=year_actual,month=mes_actual,day=dia_actual)
filt=filt[filt["inicio agendado"]<fecha]


# creamos nuevas variables que nos pueden dar mucho mas informacion a la hora del analisis,
#  como por ejemplo saber cual es el tiempo muerto que se presentó durante la reunión,
#  o cual fue el tiempo perdido debido a impuntualidad de los asistentes, etc.
filt["hora de inicio"]=0
filt["tiempo muerto inevitable"]=filt.apply(lambda x: x["tiempo de entrada inevitable"]-x["hora de ingreso"] if (x["tiempo de entrada inevitable"]!=timedelta(0)and x["hora de ingreso"]!=dt(1990,1,1) and x["hora de ingreso"]<x["tiempo de entrada inevitable"]) else timedelta(0),axis=1)
filt["hora de inicio"]=filt.apply(lambda x: x["inicio agendado"].hour,axis=1)
filt["tiempo muerto"]=filt.apply(lambda x: x["tiempo conectado"]-x["tiempo conectado asistentes"] if (int(x["tiempo conectado asistentes"].total_seconds() // 60 % 60)>0  and x["tiempo conectado"]>x["tiempo conectado asistentes"] )else timedelta(0),axis=1)
filt["diferencia tiempo"]=filt.apply(lambda x: int((x["duracion planeada"]-x["tiempo conectado"]).total_seconds() // 60 % 60) if x["duracion planeada"]>x["tiempo conectado"] else -int((x["tiempo conectado"]-x["duracion planeada"]).total_seconds() // 60 % 60),axis=1)
filt=filt.drop(columns=["tiempo de entrada inevitable"])
filt["tiempo muerto real"]=filt.apply(lambda x: x["tiempo muerto"]-x["tiempo muerto inevitable"] if x["tiempo muerto"]>x["tiempo muerto inevitable"] else timedelta(0),axis=1)

#Elimina los valores repetidos. Es decir, cuando dos filas son iguales en todo
filt=filt.drop_duplicates()
#eliminamos columnas con valores faltantes
filt=filt.dropna()

#eliminamos las tildes del nombre de las empresas y los analistas
import unicodedata
def quitar_tildes(texto):
    return ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')
filt["Nombre"]=filt.apply(lambda x:quitar_tildes(x["Nombre"]),axis=1)
filt["Empresa"]=filt.apply(lambda x: quitar_tildes(x["Empresa"]),axis=1)
filt["Empresa"]=filt.apply(lambda x: x["Empresa"].upper(), axis=1)
#tratamos por aparte una empresa que nos puede traer problemas 
# a la hora de eliminar palabras que no aportan al nombre de la empresa
filt["Empresa"]=filt.apply(lambda x:"ASESORÍA MINERA" if x["Empresa"]=='ASESORIA SST, ASESORIA MINERA' else x["Empresa"],axis=1)

#Eliminamos los terminos que nos aportan poco al nombre de la empresa
def eliminar_palabras(nombre_empresa):
    nombre_empresa=nombre_empresa.replace("ASESORIAS","")
    nombre_empresa=nombre_empresa.replace("ASESORIA","")
    nombre_empresa=nombre_empresa.replace("SISTEGRA","")
    nombre_empresa=nombre_empresa.replace("-","")
    nombre_empresa=nombre_empresa.replace("PESV","")
    nombre_empresa=nombre_empresa.replace("SST","")
    nombre_empresa=nombre_empresa.replace(".","")
    nombre_empresa=nombre_empresa.replace(",","")
    nombre_empresa=nombre_empresa.replace("1CASTROTCHERASSI","CASTROTCHERASSI")
    nombre_empresa=nombre_empresa.replace("2025","")
    nombre_empresa=nombre_empresa.replace("360","TRES SESENTA")
    nombre_empresa=nombre_empresa.replace("28","")
    nombre_empresa = " ".join(nombre_empresa.split())
    return nombre_empresa
filt["Empresa"]=filt.apply(lambda x:eliminar_palabras(x["Empresa"]),axis=1)
#Colocamos sin especificar para el nombre de las empresas 
# que no tenian un nombre significativo
filt["Empresa"]=filt.apply(lambda x:"Sin especificar" if x["Empresa"]=="" else x["Empresa"],axis=1)

#hacemos un poquito de limpieza en los nombres de las empresas 
# para que sean mas claros y ademas eliminemos facilmente empresas 
# que no nos aportan información. Como es el caso de las reuniones 
# donde no aparece el nombre de la empresa y por tanto es informacion 
# que no nos aporta mucho en las visualizaciones
filt["Empresa"]=filt.apply(lambda x: x["Empresa"].upper(), axis=1)

#agregamos una variable nueva para hacer recuento de reuniones
filt["reuniones"]=1


heat_map_data=pd.DataFrame()
#colocamos esta condicion para evitar tomar las fechas de ingreso a las reuniones, que nos salieron en un formato invalido desde el consolidado de reuniones y que por tanto no podemos usar en python.
heat_map_data["hora de ingreso"]=filt["hora de ingreso"][filt["hora de ingreso"]>dt(year=1991,month=1,day=1)]
heat_map_data["numero reuniones"]=1
#vamos a colocar las sesiones que se van a realizar en su mayor parte para la siguiente hora en la siguiente hora y lo demas solo se redondea a una hora exacta para facilitar la comprension del heat map y obtener mejores calculos
heat_map_data["hora redondeo"]=heat_map_data.apply(lambda x:(x["hora de ingreso"]+timedelta(minutes=15)).replace(minute=0,second=0,microsecond=0),axis=1)
heat_map_group=heat_map_data.groupby(["hora redondeo"])["numero reuniones"].sum().reset_index()
heat_map_group["hora"]=heat_map_group.apply(lambda x:(x["hora redondeo"]).hour,axis=1)
heat_map_group["dia de la semana"]=heat_map_group.apply(lambda x:(x["hora redondeo"]).strftime("%A"),axis=1)
heat_map_group["year"]=heat_map_group.apply(lambda x:(x["hora redondeo"]).year,axis=1)
heat_map_group["numero semana"]=heat_map_group.apply(lambda x:(x["hora redondeo"]).isocalendar().week,axis=1)
dic_semana={"Monday":"Lunes","Tuesday":"Martes","Wednesday":"Miercoles",
            "Thursday":"Jueves","Friday":"Viernes","Saturday":"Sabado"
            }
heat_map_group["dia de la semana"]=heat_map_group.apply(lambda x:dic_semana[x["dia de la semana"]],axis=1)
empresas=filt["Empresa"].sort_values(ascending=True).unique()
analistas=filt["Nombre"].unique()
from dash import html, dcc, State,Input, Output,callback, Dash
import plotly.express as px
import pandas as pd 
import dash_bootstrap_components as dbc
import re
import plotly.graph_objects as go
import numpy as np
app=Dash(__name__,external_stylesheets=[dbc.themes.BOOTSTRAP],suppress_callback_exceptions=True)
server=app.server
layout_heat_map=dbc.Container([
    dbc.Row([
        dbc.Col([
            dbc.InputGroup([
                dbc.Input(
                    id="year_bar",
                    type="text",
                    placeholder="Año"
                    )
            ], className="mb-3")
        ]),
        dbc.Col([
            dbc.InputGroup([
                dbc.Input(
                    id="month_bar",
                    type="text",
                    placeholder="Mes"
                    )
            ], className="mb-3")
        ]),
        dbc.Col([
            dbc.InputGroup([
                dbc.Input(
                    id="day_bar",
                    type="text",
                    placeholder="Día"
                    )
            ], className="mb-3")
        ])
    
    ]),
    dbc.Row([
        dbc.Col([
            dbc.Button(
                "Ingresar",
                id="button_heat",
                color="primary",
                className="mb_4"
            )
        ],width={"size": 6, "offset": 5})
    ]),
    dbc.Row([
        dbc.Col([
            dcc.Graph(figure={}, id="heat_map")
        ],width=20,md=10)
    ]),
    dbc.Row([
        dbc.Col([
            dcc.Graph(figure={},id="pie_dias")
        ]),
        dbc.Col([
            dcc.Graph(figure={},id="pie_horas")
        ])
    ])
])
layout_analistas=dbc.Container([
    dbc.Row([
        dbc.Col([
            dcc.Input(id="empleado_busqueda", 
                      type="text", 
                      placeholder="Ingresa el nombre del empleado",
                      style={
                        'width': '400px',     
                        'height': '40px',      
                        'fontSize': '18px',    
                        'padding': '10px'      
                      }
                      ),  
        ]),
        dbc.Col([
            dbc.Select(
                id="drawdown_analistas",
                options=analistas,
                value=analistas[0]
            ),
        ])
        
    ],class_name="mb-2"),
    
    dbc.Row([
        dbc.Col([
          
          dbc.Button("Seleccionar",id="boton_analistas",color="primary")
        ],width={"size": 4, "offset": 1})
    ]), 
    dbc.Row([
        dbc.Col([
            dcc.Graph(figure={},id="line_analistas_tiempo")
        ],width=12,md=6),
        dbc.Col([
            dcc.Graph(figure={},id="bar_analistas_perdido")
        ])
    ]),
    dbc.Row([
        dbc.Col([
            dcc.Graph(figure={},id="scatter_analistas")
        ]),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.P("Promedio de tiempo en conexiones", className="card-title",id="titulo_carta"),
                    html.H4("45 minutos",className="card_text",id="promedio_carta"),
                    html.P("+0%",className="card-text text-decoration-underline",id="aumento"),
                    
                ])
            ],style={"width": "200px", "height": "225px","margin-top":"150px"})
        ]),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.P("Promedio de tiempo de conexiones sin contar retrasos",className="card-title", id="titulo_sin_ret"),
                    html.H4("45 minutos",className="card-text",id="promedio_sin_ret"),
                    html.P("0%",className="card-text text-decoration-underline",id="aumento_sin_ret")
                    
                ])
            ],style={"width":"200px","height":"225px"})
        ])
    ])
])
layout_empresa=dbc.Container([
    dbc.Row([
        dbc.Col([
            dcc.Input(id="empresa_busqueda", 
                      type="text", 
                      placeholder="Ingresa el nombre de la empresa",
                      style={
                        'width': '400px',     
                        'height': '40px',      
                        'fontSize': '18px',    
                        'padding': '10px'      
                      }
                      ),
        ]),
        dbc.Col([
            dbc.Select(id="drawdown_emp",
                       options=empresas,
                       value=empresas[0])
        ])
    ],class_name="mb-2"),
    dbc.Row([
        dbc.Col([
            dbc.Button("Seleccionar",id="boton_empresas",color="primary")
        ],width={"size": 4, "offset": 1})
    ]),
    dbc.Row([
        dbc.Col([
            dcc.Graph(figure={},id="bubble_empresas")
        ],width=12,md=6)
    ]),
    dbc.Row([
        dbc.Col([
            dcc.Graph(id="tacometro_minutos_retraso")
        ]),
        dbc.Col([
            dcc.Graph(id="tacometro_tiempo_reuniones")
        ])
    ]), 
    dbc.Row([
        dbc.Col([
            dcc.Graph(id="cluster_empresas")
        ])
    ])
])

app.layout=dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H1("Dashboard analisis de rendimiento analistas", style={"text-align": "center"}),
            dbc.Select(
                id="layout_selector",
                options=[{"label": "Mapa de Calor numero de reuniones semanales", "value": "layout_heat_map"},
                         {"label":"Información sobre la empresa","value":"layout_empresa"},
                    {"label": "Información sobre el analista", "value": "layout_analistas"}],
                value="layout_analistas"
            ),
            html.Div(id='layout_output')
        ])
    ])

], fluid=True)

@callback(
    Output("layout_output","children"),
    Input("layout_selector","value"),
)
def actualizar_layout(layout_selected):
    if layout_selected=="layout_analistas":
        return layout_analistas
    elif   layout_selected=="layout_heat_map":
        return layout_heat_map
    elif layout_selected=="layout_empresa":
        return layout_empresa
@callback(
    Output("heat_map","figure"),
    Output("pie_dias","figure"),
    Output("pie_horas","figure"),
    Input('button_heat', 'n_clicks'),
    [State('year_bar', 'value'),
     State('month_bar', 'value'),
     State('day_bar', 'value')]
)
def plot_map(n_click,year,month,day):
    if n_click is None:
        empty_heatmap = np.zeros((10, 10))
        return px.imshow(empty_heatmap, color_continuous_scale='viridis'), px.line(),px.line()
    else:
        
            year=int(year)
            month=int(month)
            day=int(day)
            dic_horas={7:"7 am",8:"8 am",9:"9 am",10:"10 am",11:"11 am",12:"12 pm",
                       13:"1 pm",14:"2 pm",15:"3 pm",16:"4 pm",17:"5 pm"}
            orden_sem=["Lunes","Martes","Miercoles","Jueves","Viernes","Sabado"]
            orden_hora=["7 am","8 am","9 am","10 am","11 am",
                       "1 pm","2 pm","3 pm","4 pm","5 pm"]
            numero_semana=dt(year,month,day).isocalendar().week
            anio=dt(year,month,day).year
            filt_heat=heat_map_group[heat_map_group["numero semana"]==numero_semana]
            filt_heat=filt_heat[filt_heat["year"]==anio]
            filt_heat["hora formato"]=filt_heat.apply(lambda x:dic_horas[x["hora"]],axis=1)
            pivot=filt_heat.pivot_table(index="dia de la semana",columns="hora formato",values='numero reuniones', aggfunc='sum')
            pivot = pivot.reindex(index=orden_sem, columns=orden_hora, fill_value=0)
            pivot = pivot.fillna(0)
            aux_horas = filt_heat.groupby(["hora formato"])["numero reuniones"].sum().reset_index()
            aux_dias=filt_heat.groupby(["dia de la semana"])["numero reuniones"].sum().reset_index()
            fig= px.imshow(pivot,
                           labels=dict(x="hora", y="dia de la semana", color="numero reuniones"),
                           x=pivot.columns,
                           y=pivot.index,
                           color_continuous_scale='Viridis',
                           aspect="auto"
                           )
            
            fig.update_layout(title="Mapa de calor Numero de reuniones por dia y hora",
                              xaxis_title="Hora",
                              yaxis_title="Día")
            fig_hora=px.pie(aux_horas,values="numero reuniones",
                            names="hora formato",
                            labels={"numero reuniones":"Número de reuniones",
                                    "hora formato":"Hora"},
                            title="Reuniones por hora")
            fig_dia=px.pie(aux_dias,values="numero reuniones",
                           names="dia de la semana",
                           labels={"numero reuniones":"Número de reuniones",
                                   "dia de la semana":"Día"},
                           title="Reuniones por día")
            return fig,fig_dia,fig_hora

         
        
@callback(
    Output("line_analistas_tiempo","figure"),
    Output("bar_analistas_perdido","figure"),
    Output("scatter_analistas","figure"),
    Output("titulo_carta","children"),
    Output("promedio_carta","children"),
    Output("aumento","children"),
    Output("aumento", "style"),
    Output("titulo_sin_ret","children"),
    Output("promedio_sin_ret","children"),
    Output("aumento_sin_ret","children"),
    Output("aumento_sin_ret","style"),
    Input("boton_analistas", 'n_clicks'),
    Input("drawdown_analistas","value"),
    [State('empleado_busqueda', 'value'),
     ]
)
def plot_graph(nclick,emp_draw,empleado):
    if nclick is None:
        aux_plot=filt[filt["Nombre"]==emp_draw]
        aux_plot=aux_plot[aux_plot["hora de ingreso"]>dt(1991,1,1)]
        
        aux_plot["minutos de conexion"]=aux_plot['tiempo conectado'].dt.total_seconds() // 60 + (aux_plot['tiempo conectado'].dt.total_seconds() % 60)/100
        aux_plot["minutos perdidos"]=aux_plot['tiempo muerto'].dt.total_seconds() // 60+ (aux_plot['tiempo muerto'].dt.total_seconds() % 60)/100
        aux_plot["minutos perdidos por causa externa"]=aux_plot['tiempo muerto inevitable'].dt.total_seconds() // 60+ (aux_plot['tiempo muerto inevitable'].dt.total_seconds() % 60)/100
        aux_card=aux_plot[aux_plot["tiempo conectado asistentes"]>timedelta(minutes=10)]
        minutos_sin_retraso=aux_plot.copy()
        minutos_sin_retraso["minutos de conexion"]=minutos_sin_retraso.apply(lambda x:(x["tiempo conectado"]-x["tiempo muerto inevitable"])if (x["tiempo conectado"]>x["tiempo muerto inevitable"])else x["tiempo conectado"],axis=1)
        minutos_sin_retraso["minutos de conexion"]=minutos_sin_retraso['minutos de conexion'].dt.total_seconds() // 60 + (minutos_sin_retraso['minutos de conexion'].dt.total_seconds() % 60)/100
        minutos_sin_retraso["tiempo conectado"]=minutos_sin_retraso.apply(lambda x:(x["tiempo conectado"]-x["tiempo muerto inevitable"])if (x["tiempo conectado"]>x["tiempo muerto inevitable"])else x["tiempo conectado"],axis=1)
        aux_card_ret=minutos_sin_retraso[minutos_sin_retraso["tiempo conectado asistentes"]>timedelta(minutes=10)]
        
        aux_plot=aux_plot.sort_values("hora de ingreso")
        promedio=aux_card["tiempo conectado"].mean()
        promedio_ret=aux_card_ret["tiempo conectado"].mean()
        if aux_card.shape[0]==0:
            titulo_card="No se encuentran conexiones efectivas de "+ emp_draw
            promedio_card="0 minutos"
            aumento_card="0%"
            style={"color":"black"}
        else:
            aux_card["year"] =aux_card["hora de ingreso"].dt.year
            aux_card["numero semana"]=aux_card.apply(lambda x:(x["hora de ingreso"]).isocalendar().week,axis=1)
            promedio_sem_ant=aux_card[(aux_card["year"]!=aux_card["year"].iloc[-1])|(aux_card["numero semana"]!=aux_card["numero semana"].iloc[-1])]
            promedio_ant=promedio_sem_ant["tiempo conectado"].mean()
            promedio_porc=(promedio.total_seconds()-promedio_ant.total_seconds())/promedio_ant.total_seconds()
            titulo_card="Promedio de tiempo en conexiones de "+ emp_draw
            promedio_card= str(round(promedio.total_seconds()/60,3)) +" minutos"
            if promedio_porc>0:
                aumento_card,style=f"+{round(promedio_porc,3)}%",{"color":"green"}
            else:
                aumento_card,style=f"{round(promedio_porc,3)}%",{"color":"red"}
        if aux_card_ret.shape[0]==0:
            titulo_card_ret="No se encuentran conexiones efectivas de "+ emp_draw
            promedio_ret="0 minutos"
            aumento_card_ret="0%"
            style_ret={"color":"black"}
        else:
            aux_card_ret["year"] =aux_card_ret["hora de ingreso"].dt.year
            aux_card_ret["numero semana"]=aux_card_ret.apply(lambda x:(x["hora de ingreso"]).isocalendar().week,axis=1)
            promedio_sem_ant_ret=aux_card_ret[(aux_card_ret["year"]!=aux_card_ret["year"].iloc[-1])|(aux_card_ret["numero semana"]!=aux_card_ret["numero semana"].iloc[-1])]
            promedio_ant_ret=promedio_sem_ant_ret["tiempo conectado"].mean()
            promedio_porc_ret=(promedio_ret.total_seconds()-promedio_ant_ret.total_seconds())/promedio_ant_ret.total_seconds()
            titulo_card_ret="Promedio de tiempo en conexiones sin contar los retrasos para "+ emp_draw
            promedio_card_ret= str(round(promedio_ret.total_seconds()/60,3)) +" minutos"
            if promedio_porc_ret>0:
                aumento_card_ret,style_ret=f"+{round(promedio_porc_ret,3)}%",{"color":"green"}
            else:
                aumento_card_ret,style_ret=f"{round(promedio_porc_ret,3)}%",{"color":"red"}
        por_dia=aux_plot[["hora de ingreso","Empresa","tiempo conectado","tiempo conectado asistentes"]][aux_plot["hora de ingreso"]>dt(year=1991,month=1,day=1)]
        perdidas=por_dia[por_dia["tiempo conectado asistentes"]<=timedelta(minutes=10)]
        por_dia=por_dia[por_dia["tiempo conectado"]>timedelta(minutes=10)]
        por_dia=por_dia[por_dia["tiempo conectado asistentes"]>timedelta(minutes=10)]
        perdidas["numero de reuniones"]=1
        perdidas["year"] = perdidas["hora de ingreso"].dt.year
        perdidas["month"] = perdidas["hora de ingreso"].dt.month
        perdidas["day"] = perdidas["hora de ingreso"].dt.day
        if perdidas.shape[0]!=0:
          perdidas["fecha"] = perdidas.apply(lambda x: dt(x["year"], x["month"], x["day"]), axis=1)
        else:
            perdidas["fecha"]=dt(1990,1,1)
        perdidas_group=perdidas.groupby("fecha")["numero de reuniones"].sum().reset_index()
        por_dia["numero de reuniones"]=1
        por_dia["year"] = por_dia["hora de ingreso"].dt.year
        por_dia["month"] = por_dia["hora de ingreso"].dt.month
        por_dia["day"] = por_dia["hora de ingreso"].dt.day
        if por_dia.shape[0]!=0:
          por_dia["fecha"] = por_dia.apply(lambda x: dt(x["year"], x["month"], x["day"]), axis=1)
        else:
            por_dia["fecha"]=dt(1990,1,1)
        por_dia_group=por_dia.groupby(["fecha"])["numero de reuniones"].sum().reset_index()
        por_dia_group["reunion efectiva"]="Reunión realizada"
        perdidas_group["reunion efectiva"]="No se unieron a la reunión"
        por_dia_group=pd.concat([por_dia_group,perdidas_group])
        por_dia_group=por_dia_group.sort_values("fecha")
        
        fig_line=px.line(aux_plot,x="hora de ingreso",
                         y="minutos de conexion",
                         hover_data=["Empresa"] ,
                         labels={"hora de ingreso": "Fecha de conexión", "minutos de conexion": "Minutos conectado"},
                         title="Tiempo de conexión por reunion de "+str(emp_draw))
        fig_line.update_layout(showlegend=False)
        fig_line.update_layout(width=650, height=500)
        fig_line.update_traces(
            hoverlabel=dict(
            bgcolor="lavender",      
            font=dict(color="black", size=12)  
            )
        )
        
        x_inf=min(aux_plot["hora de ingreso"])
        x_sup=max(aux_plot["hora de ingreso"])
        fig_line.add_shape(
          type="line",
          x0=x_inf, y0=45,
          x1=x_sup, y1=45,
          line=dict(color="red", width=2, dash="dash")
        )
        fig_line.update_layout(
            xaxis=dict(
                rangeselector=dict(
                    buttons=list([
                        dict(count=1, label="1m", step="month", stepmode="backward"),
                        dict(count=6, label="6m", step="month", stepmode="backward"),
                        dict(count=1, label="1y", step="year", stepmode="backward"),
                        dict(step="all")
                    ])
                ),
                rangeslider=dict(visible=True),
            )
        )
        color_map = {
            "Reunión realizada": "green",
            "No se unieron a la reunión": "red"
        }
        fig_bar=px.bar(por_dia_group,x="fecha",
                       y="numero de reuniones",
                       labels={"fecha": "Día",
                                "numero de reuniones": "Número de reuniones",
                                "reunion efectiva":"¿Se realizó la reunión?"},
                       color="reunion efectiva",
                       color_discrete_map=color_map,
                       title="Numero de reuniones por día hechas por "+str(emp_draw))
        fig_bar.update_traces(
            hoverlabel=dict(
            bgcolor="palegreen",     
            font=dict(color="black", size=12)  
            )
        )
        fig_scatter= px.scatter(aux_plot, x="minutos perdidos", 
                                y="minutos perdidos por causa externa",
                                title="Disperción de los tiempos perdidos por reunion",
                                hover_data=["Empresa","hora de ingreso"] ,
                                labels={"minutos perdidos": "Minutos perdidos", "minutos perdidos por causa externa": "Minutos perdidos por impuntualidad de los asistentes"},
                                render_mode='webgl')
        fig_scatter.update_traces(
            hoverlabel=dict(
            bgcolor="SkyBlue",     
            font=dict(color="black", size=12)  
            )
        )
        fig_scatter.update_traces(marker_line=dict(width=1, color='DarkSlateGray'))
        x_max=max(aux_plot["minutos perdidos"])+1
        y_max=max(aux_plot["minutos perdidos por causa externa"])+1
        
        fig_scatter.add_shape(
          type="line",
          x0=0, y0=0,
          x1=max(x_max,y_max), y1=max(x_max,y_max),
          line=dict(color="red", width=2, dash="solid")
        )
        fig_scatter.update_layout(width=825, height=600)
        return fig_line, fig_bar, fig_scatter,titulo_card, promedio_card,aumento_card,style,titulo_card_ret, promedio_card_ret,aumento_card_ret,style_ret
        
    elif empleado.lower() not in filt["Nombre"].str.lower().unique():
            return px.line(),px.bar(),px.scatter(),"Promedio de tiempo en conexiones","45 minutos","+0%",{"color":"black"},"Promedio de tiempo en conexiones sin retrasos","45 minutos","+0%",{"color":"black"}
    else:
        def capitalize_all(palabra):
          l=palabra.split()
          palabra_cap="".join(pal.capitalize()+" " for pal in l)
          return palabra_cap[:-1]
        empleado_cap=capitalize_all(empleado)
        aux_plot=filt[filt["Nombre"]==empleado_cap]
        aux_plot=aux_plot[aux_plot["hora de ingreso"]>dt(1991,1,1)]
        aux_plot["minutos de conexion"]=aux_plot['tiempo conectado'].dt.total_seconds() // 60 + (aux_plot['tiempo conectado'].dt.total_seconds() % 60)/100
        aux_plot["minutos perdidos"]=aux_plot['tiempo muerto'].dt.total_seconds() // 60+ (aux_plot['tiempo muerto'].dt.total_seconds() % 60)/100
        aux_plot["minutos perdidos por causa externa"]=aux_plot['tiempo muerto inevitable'].dt.total_seconds() // 60+ (aux_plot['tiempo muerto inevitable'].dt.total_seconds() % 60)/100
        aux_card=aux_plot[aux_plot["tiempo conectado asistentes"]>timedelta(minutes=10)]
        minutos_sin_retraso=aux_plot.copy()
        minutos_sin_retraso["minutos de conexion"]=minutos_sin_retraso.apply(lambda x:(x["tiempo conectado"]-x["tiempo muerto inevitable"])if (x["tiempo conectado"]>x["tiempo muerto inevitable"])else x["tiempo conectado"],axis=1)
        minutos_sin_retraso["minutos de conexion"]=minutos_sin_retraso['minutos de conexion'].dt.total_seconds() // 60 + (minutos_sin_retraso['minutos de conexion'].dt.total_seconds() % 60)/100
        minutos_sin_retraso["tiempo conectado"]=minutos_sin_retraso.apply(lambda x:(x["tiempo conectado"]-x["tiempo muerto inevitable"])if (x["tiempo conectado"]>x["tiempo muerto inevitable"])else x["tiempo conectado"],axis=1)
        aux_card_ret=minutos_sin_retraso[minutos_sin_retraso["tiempo conectado asistentes"]>timedelta(minutes=10)]
        aux_plot=aux_plot.sort_values("hora de ingreso")
        promedio=aux_card["tiempo conectado"].mean()
        promedio_ret=aux_card_ret["tiempo conectado"].mean()
        if aux_card.shape[0]==0:
            titulo_card="No se encuentran conexiones efectivas de "+ emp_draw
            promedio_card="0 minutos"
            aumento_card="0%"
            style={"color":"black"}
        else:
            aux_card["year"] =aux_card["hora de ingreso"].dt.year
            aux_card["numero semana"]=aux_card.apply(lambda x:(x["hora de ingreso"]).isocalendar().week,axis=1)
            promedio_sem_ant=aux_card[(aux_card["year"]!=aux_card["year"].iloc[-1])|(aux_card["numero semana"]!=aux_card["numero semana"].iloc[-1])]
            promedio_ant=promedio_sem_ant["tiempo conectado"].mean()
            promedio_porc=(promedio.total_seconds()-promedio_ant.total_seconds())/promedio_ant.total_seconds()
            titulo_card="Promedio de tiempo en conexiones de "+ emp_draw
            promedio_card= str(round(promedio.total_seconds()/60,3)) +" minutos"
            if promedio_porc>0:
                aumento_card,style=f"+{round(promedio_porc,3)}%",{"color":"green"}
            else:
                aumento_card,style=f"{round(promedio_porc,3)}%",{"color":"red"}
        if aux_card_ret.shape[0]==0:
            titulo_card_ret="No se encuentran conexiones efectivas de "+ emp_draw
            promedio_ret="0 minutos"
            aumento_card_ret="0%"
            style_ret={"color":"black"}
        else:
            aux_card_ret["year"] =aux_card_ret["hora de ingreso"].dt.year
            aux_card_ret["numero semana"]=aux_card_ret.apply(lambda x:(x["hora de ingreso"]).isocalendar().week,axis=1)
            promedio_sem_ant_ret=aux_card_ret[(aux_card_ret["year"]!=aux_card_ret["year"].iloc[-1])|(aux_card_ret["numero semana"]!=aux_card_ret["numero semana"].iloc[-1])]
            promedio_ant_ret=promedio_sem_ant_ret["tiempo conectado"].mean()
            promedio_porc_ret=(promedio_ret.total_seconds()-promedio_ant_ret.total_seconds())/promedio_ant_ret.total_seconds()
            titulo_card_ret="Promedio de tiempo en conexiones sin contar los retrasos para "+ emp_draw
            promedio_card_ret= str(round(promedio_ret.total_seconds()/60,3)) +" minutos"
            if promedio_porc_ret>0:
                aumento_card_ret,style_ret=f"+{round(promedio_porc_ret,3)}%",{"color":"green"}
            else:
                aumento_card_ret,style_ret=f"{round(promedio_porc_ret,3)}%",{"color":"red"}
        por_dia=aux_plot[["hora de ingreso","Empresa","tiempo conectado","tiempo conectado asistentes"]][aux_plot["hora de ingreso"]>dt(year=1991,month=1,day=1)]
        perdidas=por_dia[por_dia["tiempo conectado asistentes"]<=timedelta(minutes=10)]
        por_dia=por_dia[por_dia["tiempo conectado"]>timedelta(minutes=10)]
        por_dia=por_dia[por_dia["tiempo conectado asistentes"]>timedelta(minutes=10)]
        perdidas["numero de reuniones"]=1
        perdidas["year"] = perdidas["hora de ingreso"].dt.year
        perdidas["month"] = perdidas["hora de ingreso"].dt.month
        perdidas["day"] = perdidas["hora de ingreso"].dt.day
        perdidas["fecha"] = perdidas.apply(lambda x: dt(x["year"], x["month"], x["day"]), axis=1)
        perdidas_group=perdidas.groupby("fecha")["numero de reuniones"].sum().reset_index()
        por_dia["numero de reuniones"]=1
        por_dia["year"] = por_dia["hora de ingreso"].dt.year
        por_dia["month"] = por_dia["hora de ingreso"].dt.month
        por_dia["day"] = por_dia["hora de ingreso"].dt.day
        por_dia["fecha"] = por_dia.apply(lambda x: dt(x["year"], x["month"], x["day"]), axis=1)
        por_dia_group=por_dia.groupby(["fecha"])["numero de reuniones"].sum().reset_index()
        por_dia_group["reunion efectiva"]="Reunión realizada"
        perdidas_group["reunion efectiva"]="No se unieron a la reunión"
        por_dia_group=pd.concat([por_dia_group,perdidas_group])
        por_dia_group=por_dia_group.sort_values("fecha")
        fig_line=px.line(aux_plot,x="hora de ingreso",
                         y="minutos de conexion",
                         hover_data=["Empresa"] ,
                         labels={"hora de ingreso": "Fecha de conexión", "minutos de conexion": "Minutos conectado"},
                         title="Tiempo de conexión por reunion de "+str(empleado_cap))
        fig_line.update_traces(
            hoverlabel=dict(
            bgcolor="lavender",      
            font=dict(color="black", size=12)  
            )
        )
        fig_line.update_traces(
           marker_color="purple" 
        )
        x_inf=min(aux_plot["hora de ingreso"])
        x_sup=max(aux_plot["hora de ingreso"])
        fig_line.add_shape(
          type="line",
          x0=x_inf, y0=45,
          x1=x_sup, y1=45,
          line=dict(color="red", width=2, dash="dash")
        )
        fig_line.update_layout(
            xaxis=dict(
                rangeselector=dict(
                    buttons=list([
                        dict(count=1, label="1m", step="month", stepmode="backward"),
                        dict(count=6, label="6m", step="month", stepmode="backward"),
                        dict(count=1, label="1y", step="year", stepmode="backward"),
                        dict(step="all")
                    ])
                ),
                rangeslider=dict(visible=True),
            )
        )
        color_map = {
            "Reunión realizada": "green",
            "No se unieron a la reunión": "red"
        }
        fig_bar=px.bar(por_dia_group,x="fecha",
                       y="numero de reuniones",
                       labels={"fecha": "Día",
                                "numero de reuniones": "Número de reuniones",
                                "reunion efectiva":"¿Se realizó la reunión?"},
                       color="reunion efectiva",
                       color_discrete_map=color_map,
                       title="Numero de reuniones por día hechas por "+str(empleado_cap))
        fig_bar.update_traces(
            hoverlabel=dict(
            bgcolor="palegreen",     
            font=dict(color="black", size=12)  
            )
        )
        fig_scatter= px.scatter(aux_plot, x="minutos perdidos", 
                                y="minutos perdidos por causa externa",
                                title="Disperción de los tiempos perdidos por reunion",
                                hover_data=["Empresa","hora de ingreso"] ,
                                labels={"minutos perdidos": "Minutos perdidos", "minutos perdidos por causa externa": "Minutos perdidos por impuntualidad de los asistentes"},
                                render_mode='webgl')
        fig_scatter.update_traces(
            hoverlabel=dict(
            bgcolor="SkyBlue",     
            font=dict(color="black", size=12)  
            )
        )
        fig_scatter.update_traces(marker_line=dict(width=1, color='DarkSlateGray'))
        x_max=max(aux_plot["minutos perdidos"])+1
        y_max=max(aux_plot["minutos perdidos por causa externa"])+1
        
        fig_scatter.add_shape(
          type="line",
          x0=0, y0=0,
          x1=max(x_max,y_max), y1=max(x_max,y_max),
          line=dict(color="red", width=2, dash="solid")
        )
        fig_scatter.update_layout(width=825, height=600)
        return fig_line, fig_bar, fig_scatter, titulo_card, promedio_card,aumento_card,style, titulo_card_ret, promedio_card_ret,aumento_card_ret,style_ret
@callback(
    Output("bubble_empresas","figure"),
    Output("tacometro_minutos_retraso","figure"),
    Output("tacometro_tiempo_reuniones","figure"),
    Output("cluster_empresas","figure"),
    Input("boton_empresas","n_clicks"),
    Input("drawdown_emp","value"),
    [State("empresa_busqueda","value")]
)
def plot_emp(n_click,empresa_draw,empresa_selected):
    if n_click is None:
        empresas_prom=filt.groupby(["Empresa"])[["tiempo muerto inevitable","tiempo conectado asistentes"]].mean().reset_index()
        empresas_prom["tiempo conectado minutos"]=empresas_prom['tiempo conectado asistentes'].dt.total_seconds() // 60+ (empresas_prom['tiempo conectado asistentes'].dt.total_seconds() % 60)/100
        faltas=filt[filt["tiempo conectado asistentes"]<timedelta(minutes=5)]
        empresas_faltas_sum=faltas.groupby(["Empresa"])[["reuniones"]].sum().reset_index()
        empresas_sum=filt.groupby(["Empresa"])[["reuniones","tiempo conectado","tiempo conectado asistentes"]].sum().reset_index()
        empresas_sum["Porcentaje tiempo efectivo"]=empresas_sum.apply(lambda x:round((x["tiempo conectado asistentes"]/x["tiempo conectado"])*100,3) if (x["tiempo conectado"]>x["tiempo conectado asistentes"])else 100, axis=1)
        empresas_faltas_sum["numero de faltas"]=empresas_faltas_sum["reuniones"]
        empresas_faltas_sum=empresas_faltas_sum.drop(columns=["reuniones"])
        empresas_sum=pd.merge(empresas_sum,empresas_faltas_sum,on="Empresa",how="outer")
        empresas_sum = empresas_sum.fillna(0)
        empresas_sum["Porcentaje de faltas"]=empresas_sum.apply(lambda x:round((x["numero de faltas"]/x["reuniones"])*100,3) if (x["reuniones"]>0)else 0, axis=1)
        empresas_sum=empresas_sum.drop(columns=["tiempo conectado asistentes","tiempo conectado"])
        recuento_empresas=pd.merge(empresas_prom,empresas_sum,on="Empresa",how="inner")
        recuento_empresas["inverso tiempo efectivo"]=101-recuento_empresas["Porcentaje tiempo efectivo"]
        recuento_empresas["tiempo muerto minutos"]=recuento_empresas['tiempo muerto inevitable'].dt.total_seconds() // 60+ (recuento_empresas['tiempo muerto inevitable'].dt.total_seconds() % 60)/100 
        recuento_empresas["color"]=np.where(recuento_empresas["Empresa"]==empresa_draw, "red","blue")
        #Vamos a entrenar un modelo de Kmeans para determinar la mejor forma de separar las empresas agrupandolas segun su comportamiento.
        from sklearn.preprocessing import StandardScaler
        from sklearn.cluster import KMeans
        data_with_names=recuento_empresas[["Empresa","tiempo conectado minutos","tiempo muerto minutos","Porcentaje de faltas"]]
        data=recuento_empresas[["tiempo conectado minutos","tiempo muerto minutos","Porcentaje de faltas"]]
        scaler=StandardScaler()
        data_scaled=scaler.fit_transform(data)
        k_means=KMeans(n_clusters=4, init="k-means++", max_iter=300, n_init=10, random_state=0)
        data['cluster'] = k_means.fit_predict(data_scaled) 
        data["Empresa"]=data_with_names["Empresa"] 
        data["grupo"]=data.apply(lambda x: "Comportamiento normal" if x["cluster"]==0 else ("Llega con retraso, pero no suele faltar" if x["cluster"]==1 else ("Asiste frecuentemente pero sus reuniones tardan mucho tiempo" if x["cluster"]==2 else "Falta con mucha frecuencia")), axis=1)
        fig_scatter_emp=px.scatter(recuento_empresas,
            x="tiempo muerto minutos",
            y="reuniones",
            hover_data=["Empresa"],
            title="Dispersion del número de reuniones con respecto al tiempo perdido por impuntualidad",
            size="inverso tiempo efectivo",
            labels={"tiempo muerto minutos":"Tiempo de retraso promedio",
                        "reuniones":"Número de reuniones",
                        "inverso tiempo efectivo":"Porcentaje de tiempo desaprovechado"
                    },
            size_max=40,
            color="color"
            )
        fig_scatter_emp.update_layout(width=1200, height=650)
        fig_tac_pro=go.Figure(
                go.Indicator(
                mode="gauge+number",
                title={'text': "Promedio de tiempo de impuntualidad", 'font': {'size': 24}},
                value=float(recuento_empresas["tiempo muerto minutos"][recuento_empresas["Empresa"]==empresa_draw]),
                gauge={
                  'axis': {'range': [0, 20]},
                  'steps': [
                  {'range': [0, 3], 'color': "lightgreen"},
                  {'range': [3, 5], 'color': "orange"},
                  {'range': [5, 20], 'color': "red"}
                  ],
                  'threshold': {'value': 90}
                }
           )
        )
        fig_tac_min=go.Figure(
        go.Indicator(
            mode="gauge+number",
            title={'text': "Promedio de tiempo conectado", 'font': {'size': 24}},
            value=float(empresas_prom["tiempo conectado minutos"][empresas_prom["Empresa"]==empresa_draw]),
            gauge={
                'axis': {'range': [0, 100]},
                'steps': [
                    {'range': [0, 15], 'color': "red"},
                    {'range': [15, 30], 'color': "orange"},
                    {'range': [30, 55], 'color': "lightgreen"},
                    {'range': [55, 100], 'color': "red"}
                ],
                'threshold': {'value': 90}
            }
        )
        )
        
        fig = go.Figure(data=[go.Scatter3d(
            x=data["tiempo conectado minutos"],
            y=data["tiempo muerto minutos"],
            z=data["Porcentaje de faltas"],
            marker=dict(color=data["cluster"]),
            mode='markers',
            opacity=0.2,
            hovertext=data.apply(
                lambda row: f"<b>Empresa:</b> {row['Empresa']}<br>"  
                        f"<b>Clasificación:</b> {row['grupo']}<br>"
                        f"<b>Tiempo de conexión promedio:</b> {row['tiempo conectado minutos']:.1f} minutos<br>"
                        f"<b>Tiempo de retraso promedio:</b> {row['tiempo muerto minutos']:.1f} minutos<br>"
                        f"<b>Ausencias:</b> {row['Porcentaje de faltas']:.1f}%",
                axis=1
            ),
            hovertemplate="%{hovertext}<extra></extra>"
        )])
        fig.update_layout(width=1200, height=700)

        return fig_scatter_emp,fig_tac_pro,fig_tac_min,fig

         
    def clean(text):
        return re.sub(r"[^a-zA-Z0-9]", "",text).lower()
    empresas=filt["Empresa"].unique()
    empresas_clean=[clean(elemento) for elemento in empresas]
    empresas_dict={empresas_clean[i]:empresas[i] for i in range(len(empresas))}
    posibles_valores=list(empresas_dict.keys())
    empresa=""
    for emp in posibles_valores:
        if clean(empresa_selected) in emp:
                empresa=emp
                break
    if empresa=="":
        return px.scatter(),px.line(),px.line()
    
    empresas_prom=filt.groupby(["Empresa"])[["tiempo muerto inevitable","tiempo conectado asistentes"]].mean().reset_index()
    empresas_prom["tiempo conectado minutos"]=empresas_prom['tiempo conectado asistentes'].dt.total_seconds() // 60+ (empresas_prom['tiempo conectado asistentes'].dt.total_seconds() % 60)/100
    faltas=filt[filt["tiempo conectado asistentes"]<timedelta(minutes=5)]
    empresas_faltas_sum=faltas.groupby(["Empresa"])[["reuniones"]].sum().reset_index()
    empresas_sum=filt.groupby(["Empresa"])[["reuniones","tiempo conectado","tiempo conectado asistentes"]].sum().reset_index()
    empresas_sum["Porcentaje tiempo efectivo"]=empresas_sum.apply(lambda x:round((x["tiempo conectado asistentes"]/x["tiempo conectado"])*100,3) if (x["tiempo conectado"]>x["tiempo conectado asistentes"])else 100, axis=1)
    empresas_faltas_sum["numero de faltas"]=empresas_faltas_sum["reuniones"]
    empresas_faltas_sum=empresas_faltas_sum.drop(columns=["reuniones"])
    empresas_sum=pd.merge(empresas_sum,empresas_faltas_sum,on="Empresa",how="outer")
    empresas_sum = empresas_sum.fillna(0)
    empresas_sum["Porcentaje de faltas"]=empresas_sum.apply(lambda x:round((x["numero de faltas"]/x["reuniones"])*100,3) if (x["reuniones"]>0)else 0, axis=1)
    empresas_sum=empresas_sum.drop(columns=["tiempo conectado asistentes","tiempo conectado"])
    recuento_empresas=pd.merge(empresas_prom,empresas_sum,on="Empresa",how="inner")
    recuento_empresas["inverso tiempo efectivo"]=101-recuento_empresas["Porcentaje tiempo efectivo"]
    recuento_empresas["tiempo muerto minutos"]=recuento_empresas['tiempo muerto inevitable'].dt.total_seconds() // 60+ (recuento_empresas['tiempo muerto inevitable'].dt.total_seconds() % 60)/100 
    recuento_empresas["color"]=np.where(recuento_empresas["Empresa"]==empresas_dict[empresa], "red","blue")
    
    #Vamos a entrenar un modelo de Kmeans para determinar la mejor forma de separar las empresas agrupandolas segun su comportamiento.
    from sklearn.preprocessing import StandardScaler
    from sklearn.cluster import KMeans
    data_with_names=recuento_empresas[["Empresa","tiempo conectado minutos","tiempo muerto minutos","Porcentaje de faltas"]]
    data=recuento_empresas[["tiempo conectado minutos","tiempo muerto minutos","Porcentaje de faltas"]]
    scaler=StandardScaler()
    data_scaled=scaler.fit_transform(data)
    k_means=KMeans(n_clusters=4, init="k-means++", max_iter=300, n_init=10, random_state=0)
    data['cluster'] = k_means.fit_predict(data_scaled) 
    data["Empresa"]=data_with_names["Empresa"] 
    data["grupo"]=data.apply(lambda x: "Comportamiento normal" if x["cluster"]==0 else ("Llega con retraso, pero no suele faltar" if x["cluster"]==1 else ("Asiste frecuentemente pero sus reuniones tardan mucho tiempo" if x["cluster"]==2 else "Falta con mucha frecuencia")), axis=1)
    fig_scatter_emp=px.scatter(recuento_empresas,
                x="tiempo muerto minutos",
                y="reuniones",
                hover_data=["Empresa"],
                title="Dispersion del número de reuniones con respecto al tiempo perdido por impuntualidad",
                size="inverso tiempo efectivo",
                labels={"tiempo muerto minutos":"Tiempo de retraso promedio",
                        "reuniones":"Número de reuniones",
                        "inverso tiempo efectivo":"Porcentaje de tiempo desaprovechado"
                        },
                size_max=40,
                color="color"
            )
    fig_scatter_emp.update_layout(width=1200, height=650)
    fig_tac_pro=go.Figure(
        go.Indicator(
            mode="gauge+number",
            title={'text': "Promedio de tiempo de impuntualidad", 'font': {'size': 24}},
            value=float(recuento_empresas["tiempo muerto minutos"][recuento_empresas["Empresa"]==empresas_dict[empresa]]),
            gauge={
                'axis': {'range': [0, 20]},
                'steps': [
                    {'range': [0, 3], 'color': "lightgreen"},
                    {'range': [3, 5], 'color': "orange"},
                    {'range': [5, 20], 'color': "red"}
                ],
                'threshold': {'value': 90}
            }
        )
    )
    fig_tac_min=go.Figure(
        go.Indicator(
            mode="gauge+number",
            title={'text': "Promedio de tiempo conectado", 'font': {'size': 24}},
            value=float(empresas_prom["tiempo conectado minutos"][empresas_prom["Empresa"]==empresas_dict[empresa]]),
            gauge={
                'axis': {'range': [0, 100]},
                'steps': [
                    {'range': [0, 15], 'color': "red"},
                    {'range': [15, 30], 'color': "orange"},
                    {'range': [30, 55], 'color': "lightgreen"},
                    {'range': [55, 100], 'color': "red"}
                ],
                'threshold': {'value': 90}
            }
        )
    )
    
    fig = go.Figure(data=[go.Scatter3d(
            x=data["tiempo conectado minutos"],
            y=data["tiempo muerto minutos"],
            z=data["Porcentaje de faltas"],
            marker=dict(color=data["cluster"]),
            mode='markers',
            opacity=0.2,
            hovertext=data.apply(
                lambda row: f"<b>Empresa:</b> {row['Empresa']}<br>"  
                        f"<b>Clasificación:</b> {row['grupo']}<br>"
                        f"<b>Tiempo de conexión promedio:</b> {row['tiempo conectado minutos']:.1f} minutos<br>"
                        f"<b>Tiempo de retraso promedio:</b> {row['tiempo muerto minutos']:.1f} minutos<br>"
                        f"<b>Ausencias:</b> {row['Porcentaje de faltas']:.1f}%",
                axis=1
            ),
            hovertemplate="%{hovertext}<extra></extra>"
    )])
    fig.update_layout(width=1200, height=700)

    return fig_scatter_emp,fig_tac_pro,fig_tac_min,fig

if __name__ == '__main__':
    app.run(debug=True)

       
       
