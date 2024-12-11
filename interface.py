import streamlit as st

st.title('Генерация допустимых размещений контейнеров на ЖД платформы')
st.subheader('Загрузка файла')
uploaded_file = st.file_uploader("Upload a .bak file", type=['bak'])
if uploaded_file is not None:
    df = uploaded_file
    st.write("success")
st.subheader('Начать генерацию')
button = st.button('Начать генерацию', key='1')
if button:
    st.write('Началась генерация')