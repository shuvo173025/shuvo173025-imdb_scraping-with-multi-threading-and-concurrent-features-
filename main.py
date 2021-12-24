from bs4 import BeautifulSoup
import requests
import pandas as pd
import time
import threading
import concurrent.futures
import random



a = 1
c = 2
b = 0
d = 0

# movie_description_link = []

#this part is working but got block while scraping
#so i copy this part and run it in colab
#luckly i got all data so i comment this part for here6
#
# next = ''
# while (next):
#     url = next
#     response = requests.get(url)
#     soup = BeautifulSoup(response.content, 'html.parser')
#
#     movie_data = soup.find_all('div', attrs={'class': 'lister-item mode-advanced'})
#
#     for store in movie_data:
#         temp = store.h3.a.get('href')
#         link = 'https://www.imdb.com' + temp
#         movie_description_link.append(link)
#
#     print(a, " link done ")
#     a += 1
#
#     time.sleep(random.randint(3,10))
#
#     next_link = soup.find_all('a', attrs={'class': 'lister-page-next next-page'})
#     if next_link != []:
#         for store in next_link:
#             temp = store.get('href')
#             next = 'https://www.imdb.com' + temp
#     else:
#         next = 0



#making csv file for sstoring data
movie_name = []
director = []
writer = []
tagline = []
genres = []
year = []
length = []
budget = []
gross_domistic = []
gross_worldwide = []
language = []
production_company = []
cast = []
storyline = []
imdb_suggetion = []
top_review = []
imdb_rating = []
keywords = []
b = 0

dic = {"Movie_Name": movie_name,
        "Movie_Director": director,
        "Movie_Writer": writer,
        "Movie_Tag_Line": tagline,
        "Movie_Genres": genres,
        "Movie_Year": year,
        "Movie_Length": length,
        "Movie_Budget": budget,
        "Movie_Gross_Domistic": gross_domistic,
        "Movie_Gross_Worldwide": gross_worldwide,
        "Movie_Language": language,
        "Movie_Production_Company": production_company,
        "Movie_Cast": cast,
        "Movie_Story_Line": storyline,
        "Movie_IMDB_Suggetion": imdb_suggetion,
        "Movie_Top_Review": top_review,
        "Movie_IMDB_Rating": imdb_rating,
        "Movie_Keywords": keywords
        }

df = pd.DataFrame(dic)
df.to_csv("movie_info.csv", mode='w', encoding='utf8', header=False, index=False)



#when i try to find all the movie link using my pycharm i got block from imdb server
#after several time i find a solution that i try to find all movie link using colab
# from colab i successfully got all the movie link then take all the link in txt file

#extracting txt file and put the links into my previous list
with open("mv-links.txt", "r", encoding='utf8') as f:
  a = f.read()
movie_description_link = a.split(',')



print(len(movie_description_link))

    #writing all movie link into txt file

# with open("mv-links.txt", 'w', encoding='utf8') as f:
#     for links_info in movie_description_link:
#         f.write(f"{links_info},")



# function for concurrent features
def data_storing(data_list):
    time.sleep(random.randint(1,200))
    movie_name = []
    director = []
    writer = []
    tagline = []
    genres = []
    year = []
    length = []
    budget = []
    gross_domistic = []
    gross_worldwide = []
    language = []
    production_company = []
    cast = []
    storyline = []
    imdb_suggetion = []
    top_review = []
    imdb_rating = []
    keywords = []


    dic = {"Movie_Name": movie_name,
           "Movie_Director": director,
           "Movie_Writer": writer,
           "Movie_Tag_Line": tagline,
           "Movie_Genres": genres,
           "Movie_Year": year,
           "Movie_Length": length,
           "Movie_Budget": budget,
           "Movie_Gross_Domistic": gross_domistic,
           "Movie_Gross_Worldwide": gross_worldwide,
           "Movie_Language": language,
           "Movie_Production_Company": production_company,
           "Movie_Cast": cast,
           "Movie_Story_Line": storyline,
           "Movie_IMDB_Suggetion": imdb_suggetion,
           "Movie_Top_Review": top_review,
           "Movie_IMDB_Rating": imdb_rating,
           "Movie_Keywords": keywords
           }

    df = pd.DataFrame(dic)
    # df.to_csv("movie_info.csv", mode='w', encoding='utf8', header=False, index=False)

    # for info in data_list:
    global b,d
    b += 1
    d += 1
    url_link = str(data_list)
    url = url_link
    # print(url)
    time.sleep(random.randint(1, 200))
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    # movie name scraping
    try:
        temp = soup.find('div', attrs={'class': 'TitleBlock__TitleContainer-sc-1nlhx7j-1 jxsVNt'})
        name = temp.h1.text
        movie_name.append(name)
    except:
        null_data = 'null'
        movie_name.append(null_data)
        print(d, "number movie data not Found.")


#movie director data scraping
    try:
        a = ""
        derector_info = soup.find('section',attrs={'data-testid':'title-cast'}).find('ul',attrs={'class':'ipc-metadata-list ipc-metadata-list--dividers-all StyledComponents__CastMetaDataList-y9ygcu-10 cbPPkN ipc-metadata-list--base'}).find('div',attrs={'class':'ipc-metadata-list-item__content-container'}).find_all('a',attrs={'class':'ipc-metadata-list-item__list-content-item ipc-metadata-list-item__list-content-item--link'})
        for data in derector_info:
            a += data.text + " "
        director.append(a)
    except:
        null_data = 'null'
        director.append(null_data)


    # movie writer data scraping
    try:
        a = ""
        h = []
        writer_info = soup.find('section',attrs={'data-testid':'title-cast'}).find('ul',attrs={'class':'ipc-metadata-list ipc-metadata-list--dividers-all StyledComponents__CastMetaDataList-y9ygcu-10 cbPPkN ipc-metadata-list--base'}).find_all('ul',attrs={'class':'ipc-inline-list ipc-inline-list--show-dividers ipc-inline-list--inline ipc-metadata-list-item__list-content base'})
        for data in writer_info:
            h.append(data.text)
        h.pop(0)
        writer.append(str(h))
    except:
        null_data = 'null'
        writer.append(null_data)



#movie tagline scraping
    try:
        tagline_info = soup.find('section',attrs={'data-testid':'Storyline'}).find('li',attrs={'data-testid':'storyline-taglines'}).find('span',attrs={'class':'ipc-metadata-list-item__list-content-item'})
        tagline.append(str(tagline_info.text))
    except:
        null_data = 'null'
        tagline.append(null_data)



  #movie genres data scraping
    try:
        genres_info = soup.find('section',attrs={'data-testid':'Storyline'}).find('li',attrs={'data-testid':'storyline-genres'}).find_all('ul',attrs={'class':'ipc-inline-list ipc-inline-list--show-dividers ipc-inline-list--inline ipc-metadata-list-item__list-content base'})
        for data in genres_info:
            genres.append(str(data.text))
    except:
        null_data = 'null'
        genres.append(null_data)



    #movie year data scraping
    try:
        temp3 = soup.find('span', attrs={'class': 'TitleBlockMetaData__ListItemText-sc-12ein40-2 jedhex'})
        year_data = temp3.text
        year.append(year_data)
    except:
        null_data = 'null'
        year.append(null_data)

    # movie length data scraping
    try:
        movie_length = soup.find('div', attrs={'data-testid': 'title-techspecs-section'}).find('div', attrs={
            'class': 'ipc-metadata-list-item__content-container'}).text
        length.append(str(movie_length))
    except:
        null_data = 'null'
        length.append(null_data)



    # movie budget data scraping
    try:
        movie_budget = soup.find('section', attrs={'data-testid': 'BoxOffice'}).find('div', attrs={
            'data-testid': 'title-boxoffice-section'}).find('li',
                                                            attrs={'data-testid': 'title-boxoffice-budget'}).find(
            'span', attrs={'class': 'ipc-metadata-list-item__list-content-item'}).text.replace('(estimated)', '')
        budget.append(movie_budget)
    except:
        null_data = 'null'
        budget.append(null_data)


    # movie domistic gross data scraping
    try:
        domistic_gross = soup.find('section', attrs={'data-testid': 'BoxOffice'}).find('div', attrs={
            'data-testid': 'title-boxoffice-section'}).find('li',
                                                            attrs={
                                                                'data-testid': 'title-boxoffice-grossdomestic'}).find(
            'span', attrs={'class': 'ipc-metadata-list-item__list-content-item'}).text
        gross_domistic.append(domistic_gross)
    except:
        null_data = 'null'
        gross_domistic.append(null_data)


    # movie world wide gross data scraping
    try:
        worldwide_gross = soup.find('section', attrs={'data-testid': 'BoxOffice'}).find('div', attrs={
            'data-testid': 'title-boxoffice-section'}).find('li', attrs={
            'data-testid': 'title-boxoffice-cumulativeworldwidegross'}).find('span', attrs={
            'class': 'ipc-metadata-list-item__list-content-item'}).text
        gross_worldwide.append(worldwide_gross)

    except:
        null_data = 'null'
        gross_worldwide.append(null_data)




    # movie language data scraping
    try:
        a = ""
        h = []
        movie_language = soup.find('section', attrs={'cel_widget_id': 'StaticFeature_Details'}).find('div', attrs={
            'data-testid': 'title-details-section'}).find('li',
                                                          attrs={'data-testid': 'title-details-languages'}).find_all(
            'li',
            attrs={
                'class': 'ipc-inline-list__item'})
        for data in movie_language:
            h.append(data.text)
        a = ','.join(h)
        language.append(str(a))
    except:
        null_data = 'null'
        language.append(null_data)



# movie production data scraping
    try:
        production = soup.find('section', attrs={'cel_widget_id': 'StaticFeature_Details'}).find('div', attrs={
            'data-testid': 'title-details-section'}).find('li', attrs={'data-testid': 'title-details-companies'}).find(
            'div', attrs={'class': 'ipc-metadata-list-item__content-container'}).text
        production_company.append(str(production))
    except:
        null_data = 'null'
        production_company.append(null_data)



  # movie cast data scraping
    try:
        a = ""
        h = []
        movie_cast = soup.find('section', attrs={'data-testid': 'title-cast'}).find('div', attrs={
            'class': 'ipc-shoveler title-cast__grid'}).find('div', attrs={
            'class': 'ipc-sub-grid ipc-sub-grid--page-span-2 ipc-sub-grid--wraps-at-above-l ipc-shoveler__grid'}).find_all(
            'a', attrs={'data-testid': 'title-cast-item__actor'})
        for data in movie_cast:
            h.append(data.text)
        a = ','.join(h)
        cast.append(str(a))
    except:
        null_data = 'null'
        cast.append(null_data)



# movie suggetion data scraping
    try:
        a = ""
        h = []
        suggetion = soup.find('section', attrs={'data-testid': 'MoreLikeThis'}).find('div',
                                                                                      attrs={
                                                                                          'class': 'ipc-shoveler'}).find(
            'div',
            attrs={'class': 'ipc-sub-grid ipc-sub-grid--page-span-2 ipc-sub-grid--nowrap ipc-shoveler__grid'}).find_all(
            'a',
            attrs={
                'class': 'ipc-poster-card__title ipc-poster-card__title--clamp-2 ipc-poster-card__title--clickable'})
        for data in suggetion:
                h.append(data.text)
        a = ','.join(h)
        suggetion.append(str(a))

    except:
        null_data = 'null'
        imdb_suggetion.append(null_data)





    # movie review data scraping
    try:
        movie_review = soup.find('section', attrs={'data-testid': 'UserReviews'}).find('div', attrs={
            'class': 'styles__MetaDataContainer-sc-12uhu9s-0 cgqHBf'}).find('div', attrs={
            'class': 'ipc-list-card--border-speech ipc-list-card UserReviewsFeaturedReview__SpeechBubble-pyneo1-1 iuPZAc ipc-list-card--base'}).find(
            'div', attrs={'data-testid': 'review-overflow'}).find('div', attrs={
            'class': 'ipc-html-content ipc-html-content--base'}).text
        top_review.append(str(movie_review))


    except:
        null_data = 'null'
        top_review.append(null_data)




  # movie rating data scraping
    try:
        movie_rating = soup.find('div', attrs={'data-testid': 'hero-rating-bar__aggregate-rating__score'}).find('span',
                                                                                                                attrs={
                                                                                                                    'class': 'AggregateRatingButton__RatingScore-sc-1ll29m0-1 iTLWoV'}).text
        imdb_rating.append(str(movie_rating))

    except:
        null_data = 'null'
        imdb_rating.append(null_data)





  # movie keywords data scraping
    try:
        a = ""
        movie_keyword = soup.find('div', attrs={'data-testid': 'storyline-plot-keywords'}).find_all('a', attrs={
            'class': 'ipc-chip ipc-chip--on-base'})
        for data in movie_keyword:
            a += data.text + ' '
        keywords.append(a)

    except:
        null_data = 'null'
        keywords.append(null_data)


    print(b, " movie data stored")


    dic = {"Movie_Name":movie_name,
            "Movie_Director":director,
            "Movie_Writer":writer,
            "Movie_Tag_Line":tagline,
            "Movie_Genres":genres,
            "Movie_Year":year,
            "Movie_Length":length,
            "Movie_Budget":budget,
            "Movie_Gross_Domistic":gross_domistic,
            "Movie_Gross_Worldwide":gross_worldwide,
            "Movie_Language":language,
            "Movie_Production_Company":production_company,
            "Movie_Cast":cast,
            "Movie_Story_Line":storyline,
            "Movie_IMDB_Suggetion":imdb_suggetion,
            "Movie_Top_Review":top_review,
            "Movie_IMDB_Rating":imdb_rating,
            "Movie_Keywords":keywords
            }

    #storing my scraping data into csv file
    df = pd.DataFrame.from_dict(dic, orient='index')
    df = df.transpose()
    df.to_csv("movie_info.csv", mode='a', encoding='utf8', index=False, header=False)


    # print(df)

    movie_name = []
    director = []
    writer = []
    tagline = []
    genres = []
    year = []
    length = []
    budget = []
    gross_domistic = []
    gross_worldwide = []
    language = []
    production_company = []
    cast = []
    storyline = []
    imdb_suggetion = []
    top_review = []
    imdb_rating = []
    keywords = []





#using thread to reducing time to collect movie data

# def multi_thread(l):
#   print("list count: ", len(l))
#   n = int(input(" chunk count: "))

#   x = [l[i:i + n] for i in range(0, len(l), n)]
#   print('chunk count ', len(x))

#   a =  [threading.Thread(target=data_storing, args=(i, )) for i in x]

#   for i in a:
#     i.start()

#   for i in a:
#     i.join()



# multi_thread(movie_description_link)


#using concurrent features its better then threading

with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
    executor.map(data_storing, movie_description_link)
