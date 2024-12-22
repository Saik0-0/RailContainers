from pathlib import Path
import streamlit as st
from combination_generator import CombinationGenerator
import datetime
import os
st.title('Генерация допустимых размещений контейнеров на ЖД платформы')
st.subheader('Загрузка файла')
uploaded_file = st.file_uploader("Upload a .bak file", type=['bak'])
if uploaded_file is not None:
    df = uploaded_file
    file_name = Path(uploaded_file.name).stem
st.subheader('Введите номер сессии, для которой необходимо произвести расчёт')
sessionID = st.number_input("Сессия №", min_value=1, max_value=400, value=1)
st.subheader('Генерация размещений')
button = st.button('Начать генерацию', key='1')
if button:
    st.write('Началась генерация ' + str(datetime.datetime.now()))
    cg = CombinationGenerator(file_name, sessionID)
    result_file = cg.create()
    if result_file is None:
        st.warning('В базе данных нет сессии с данным номером')
    else:
        st.write('Генерация окончена ' + str(datetime.datetime.now()))
        # st.write("Результат генерации для сессии №" + str(sessionID) + " сохранён в файле " + os.path.abspath("result_session_" + str(sessionID) + ".json"))
        # st.markdown(f"[Открыть файл в проводнике]({os.path.abspath("result_session_" + str(sessionID) + ".json")})")
        st.subheader("Экспорт результата")
        with open("result_session_" + str(sessionID) + ".json", 'rb') as f:
            st.download_button("Скачать файл", f, "result_session_" + str(sessionID) + ".json")
