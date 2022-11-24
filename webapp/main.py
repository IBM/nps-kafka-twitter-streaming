import streamlit as st
import pandas as pd
import numpy as np
import re
import regex as re
import enchant
import time
import plotly.express as px  # interactive charts
import streamlit as st  # 🎈 data web app development
from queries import *
from matplotlib import pyplot as plt
from wordcloud import WordCloud, STOPWORDS
import matplotlib.colors as mcolors


st.set_page_config(
    page_title="Twitter Most Trending Topics",
    page_icon="✅",
    layout='centered'
)


# dashboard title
st.title("Twitter Most Trending Topics")


# creating a single-element container
placeholder = st.empty()


# near real-time / live feed simulation
for seconds in range(200):
    with placeholder.container():
        
        #Get trending topics stored in the netezza database from the last 1 minute 
        topics = get_topics_from_netezza(int(time.time()) - 60)
        cols = [color for name, color in mcolors.TABLEAU_COLORS.items()] 

        cloud = WordCloud(
                        background_color='white',
                        width=2500,
                        height=1800,
                        max_words=8,
                        colormap='tab10',
                        color_func=lambda *args, **kwargs: cols[i],
                        prefer_horizontal=1.0)

        
        topics_list = []
        for i in topics:
            d = enchant.Dict("en_US")
            topic_names = re.findall('"(.*?)"',i)
            topic_values = re.findall(".\d+",i)
            start = 2
            topics_dict ={}
            for name in topic_names:
                topics_dict[name] = float(topic_values[start])
                start+=2

            #Check if words are valid 

            for word in list(topics_dict.keys()):
                if word =='http' or word=='follow' or not d.check(word) or len(word)<3 :
                    del topics_dict[word]
            topics_list.append(topics_dict)


        fig, axes = plt.subplots(3, 3, figsize=(10,10), sharex=True, sharey=True)
        for i, ax in enumerate(axes.flatten()):
            fig.add_subplot(ax)
            topic_words = topics_list[i]
            if len(topic_words)>0:
                cloud.generate_from_frequencies(topic_words, max_font_size=300)
                plt.gca().imshow(cloud)
                plt.gca().set_title('Topic ' + str(i), fontdict=dict(size=16))
                plt.gca().axis('off')


        plt.subplots_adjust(wspace=0, hspace=0)
        plt.axis('off')
        plt.margins(x=0, y=0)
        plt.tight_layout()
        st.pyplot(plt)
        time.sleep(60)