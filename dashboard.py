
import pandas as pd
data_horas = pd.read_excel('archivo_analisis/ensayo.xlsx')
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
data_horas

#seleccionamos las columnas mas relevantes para el analisis
filt=data_horas[["Nombre","Numero de participantes","Email","Rol","Empresa","hora de ingreso","duracion planeada","inicio agendado","tiempo conectado"]]
#creamos nuevas columnas para obtener posteriormente los tiempos muertos por reunion
filt["tiempos"]=filt.apply(lambda x: [], axis=1)
filt["tiempos inevitables"]=filt.apply(lambda x: [], axis=1)

#en este proceso vamos a almacenar los tiempos que duraron los asistentes
#  en la reunion junto con su hora de entrada
for i in range(filt.shape[0]):
    if filt.loc[i+1,"Rol"]=="Organizer":
        numero_asistentes=filt.loc[i+1,"Numero de participantes"]
        if numero_asistentes>1:
          lista=list(filt["tiempo conectado"][i+1:i+numero_asistentes])
          inevitables=list(filt["hora de ingreso"][i+1:i+numero_asistentes])
          filt.at[i+1,"tiempos"]=lista
          filt.at[i+1,"tiempos inevitables"]=inevitables
#seleccionamos como un aproximado del tiempo que duraron los asistentes 
# a la persona que mas tiempo duro dentro de la reunion y seleccionamos
#  su hora de llegada a la reunion
filt["tiempo conectado asistentes"]=filt.apply(lambda x: max(x["tiempos"])if len(x["tiempos"])>0 else 0, axis=1)
filt["tiempo de entrada inevitable"]=filt.apply(lambda x:x["tiempos inevitables"][x["tiempos"].index(max(x["tiempos"]))]if len(x["tiempos"])>0 else x["hora de ingreso"],axis=1)
#seleccionamos unicamente a los organizadores de las reuniones, 
# ya que dentro de esto se van a encontrar todas las reuniones de los analistas
filt=filt[filt["Rol"]=="Organizer"]
filt=filt.drop(columns=["Rol"])

#En esta parte arreglamos un pequeño error en los emails que se nos presenta 
# para evitar errores futuros en el procesamiento de las columnas

for i in filt.index:  
    if isinstance(filt.loc[i, "Email"], float):
        filt.loc[i, "Email"] = "None"

#Realizaremos un filtrado de los correos utilizando expresiones regulares 
# para obtener unicamente los correos de los analistas. 
# Los cuales siempre inician por analista
import re
patron  = r"^analista"
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
filt["tiempo conectado"]=filt.apply(lambda x: pd.to_timedelta(x["tiempo conectado"]),axis=1)
filt["tiempo conectado asistentes"]=filt.apply(lambda x: pd.to_timedelta(x["tiempo conectado asistentes"]),axis=1)
#hacemos un pequeño arreglo para evitar las molestias operacionales que nos puede presentar el tener System.Object[] en algunas filas, en lugar de tener una fecha
filt["tiempo de entrada inevitable"]=filt.apply(lambda x: pd.to_datetime(x["tiempo de entrada inevitable"])-timedelta(hours=5) if str(x["tiempo de entrada inevitable"])!="System.Object[]" else dt(1990,1,1) ,axis=1)
filt["hora de ingreso"]=filt.apply(lambda x: pd.to_datetime(x["hora de ingreso"])-timedelta(hours=5) if str(x["hora de ingreso"])!="System.Object[]" else  dt(1990,1,1),axis=1)

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

#eliminamos las tildes del nombre
import unicodedata
def quitar_tildes(texto):
    return ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')
filt["Nombre"]=filt.apply(lambda x:quitar_tildes(x["Nombre"]),axis=1)
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
empresas=filt["Empresa"].unique()
analistas=filt["Nombre"].unique()
from dash import html, dcc, State,Input, Output,callback, Dash
import plotly.express as px
import pandas as pd 
import dash_bootstrap_components as dbc
import re
import plotly.graph_objects as go
import numpy as np
app=Dash(__name__,external_stylesheets=[dbc.themes.BOOTSTRAP],suppress_callback_exceptions=True)
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
                    html.P("+0%",className="card-text text-decoration-underline",id="aumento")
                ])
            ],style={"width": "200px", "height": "270px","margin-top": "150px"})
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
    Input('button_heat', 'n_clicks'),
    [State('year_bar', 'value'),
     State('month_bar', 'value'),
     State('day_bar', 'value')]
)
def plot_map(n_click,year,month,day):
    if n_click is None:
        empty_heatmap = np.zeros((10, 10))
        return px.imshow(empty_heatmap, color_continuous_scale='viridis')
    else:
        
            year=int(year)
            month=int(month)
            day=int(day)
            orden_sem=["Lunes","Martes","Miercoles","Jueves","Viernes","Sabado"]
            orden_hora=[i for i in range(7,12)]+[i for i in range(13,18)]
            numero_semana=dt(year,month,day).isocalendar().week
            anio=dt(year,month,day).year
            filt_heat=heat_map_group[heat_map_group["numero semana"]==numero_semana]
            filt_heat=filt_heat[filt_heat["year"]==anio]
            pivot=filt_heat.pivot_table(index="dia de la semana",columns="hora",values='numero reuniones', aggfunc='sum')
            pivot = pivot.reindex(index=orden_sem, columns=orden_hora, fill_value=0)
            pivot = pivot.fillna(0)
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
            return fig

         
        
@callback(
    Output("line_analistas_tiempo","figure"),
    Output("bar_analistas_perdido","figure"),
    Output("scatter_analistas","figure"),
    Output("titulo_carta","children"),
    Output("promedio_carta","children"),
    Output("aumento","children"),
    Output("aumento", "style"),
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
        aux_plot=aux_plot.sort_values("hora de ingreso")
        promedio=aux_card["tiempo conectado"].mean()
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
                         title="Tiempo de conexión por reunion de "+str(emp_draw))
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
                                hover_data=["Empresa","inicio agendado"] ,
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
        return fig_line, fig_bar, fig_scatter, titulo_card, promedio_card,aumento_card,style
        #return px.line(),px.bar(),px.scatter(),"Promedio de tiempo en conexiones","45 minutos","+0%",{"color":"black"}
    elif empleado.lower() not in filt["Nombre"].str.lower().unique():
            return px.line(),px.bar(),px.scatter(),"Promedio de tiempo en conexiones","45 minutos","+0%",{"color":"black"}
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
        aux_plot=aux_plot.sort_values("hora de ingreso")
        promedio=aux_card["tiempo conectado"].mean()
        aux_card["year"] =aux_card["hora de ingreso"].dt.year
        aux_card["numero semana"]=aux_card.apply(lambda x:(x["hora de ingreso"]).isocalendar().week,axis=1)
        promedio_sem_ant=aux_card[(aux_card["year"]!=aux_card["year"].iloc[-1])|(aux_card["numero semana"]!=aux_card["numero semana"].iloc[-1])]
        promedio_ant=promedio_sem_ant["tiempo conectado"].mean()
        promedio_porc=(promedio.total_seconds()-promedio_ant.total_seconds())/promedio_ant.total_seconds()
        titulo_card="Promedio de tiempo en conexiones de "+ empleado_cap
        promedio_card= str(round(promedio.total_seconds()/60,3)) +" minutos"
        if promedio_porc>0:
            aumento_card,style=f"+{round(promedio_porc,3)}%",{"color":"green"}
        else:
            aumento_card,style=f"{round(promedio_porc,3)}%",{"color":"red"}
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
                                hover_data=["Empresa","inicio agendado"] ,
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
        return fig_line, fig_bar, fig_scatter, titulo_card, promedio_card,aumento_card,style
@callback(
    Output("bubble_empresas","figure"),
    Output("tacometro_minutos_retraso","figure"),
    Output("tacometro_tiempo_reuniones","figure"),
    Input("boton_empresas","n_clicks"),
    Input("drawdown_emp","value"),
    [State("empresa_busqueda","value")]
)
def plot_emp(n_click,empresa_draw,empresa_selected):
    if n_click is None:
        empresas_prom=filt.groupby(["Empresa"])[["tiempo muerto inevitable","tiempo conectado asistentes"]].mean().reset_index()
        empresas_prom["tiempo conectado minutos"]=empresas_prom['tiempo conectado asistentes'].dt.total_seconds() // 60+ (empresas_prom['tiempo conectado asistentes'].dt.total_seconds() % 60)/100
        empresas_sum=filt.groupby(["Empresa"])[["reuniones","tiempo conectado","tiempo conectado asistentes"]].sum().reset_index()
        empresas_sum["Porcentaje tiempo efectivo"]=empresas_sum.apply(lambda x:round((x["tiempo conectado asistentes"]/x["tiempo conectado"])*100,3) if (x["tiempo conectado"]>x["tiempo conectado asistentes"])else 100, axis=1)
        recuento_empresas=pd.merge(empresas_prom,empresas_sum,on="Empresa",how="inner")
        recuento_empresas["inverso tiempo efectivo"]=101-recuento_empresas["Porcentaje tiempo efectivo"]
        recuento_empresas["tiempo muerto minutos"]=recuento_empresas['tiempo muerto inevitable'].dt.total_seconds() // 60+ (recuento_empresas['tiempo muerto inevitable'].dt.total_seconds() % 60)/100
        recuento_empresas["color"]=np.where(recuento_empresas["Empresa"]==empresa_draw, "red","blue")
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
        fig_scatter_emp.update_layout(width=1200, height=650)

        return fig_scatter_emp,fig_tac_pro,fig_tac_min

         
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
    empresas_sum=filt.groupby(["Empresa"])[["reuniones","tiempo conectado","tiempo conectado asistentes"]].sum().reset_index()
    empresas_sum["Porcentaje tiempo efectivo"]=empresas_sum.apply(lambda x:round((x["tiempo conectado asistentes"]/x["tiempo conectado"])*100,3) if (x["tiempo conectado"]>x["tiempo conectado asistentes"])else 100, axis=1)
    recuento_empresas=pd.merge(empresas_prom,empresas_sum,on="Empresa",how="inner")
    recuento_empresas["inverso tiempo efectivo"]=101-recuento_empresas["Porcentaje tiempo efectivo"]
    recuento_empresas["tiempo muerto minutos"]=recuento_empresas['tiempo muerto inevitable'].dt.total_seconds() // 60+ (recuento_empresas['tiempo muerto inevitable'].dt.total_seconds() % 60)/100
    recuento_empresas["color"]=np.where(recuento_empresas["Empresa"]==empresas_dict[empresa], "red","blue")
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
    print(float(recuento_empresas["tiempo muerto minutos"][recuento_empresas["Empresa"]==empresas_dict[empresa]]))
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
    fig_scatter_emp.update_layout(width=1200, height=650)

    return fig_scatter_emp,fig_tac_pro,fig_tac_min

if __name__ == '__main__':
    app.run(debug=True)

       
       