# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
class FilmscraperItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    url = scrapy.Field()          # URL du film
    title = scrapy.Field()        # Titre du film
    duree = scrapy.Field()        # Durée du film
    press_rate = scrapy.Field()   # Note de la presse
    spectateur_rate = scrapy.Field()  # Note des spectateurs
    histoire = scrapy.Field()     # Description ou histoire du film
    commentaire = scrapy.Field()   # Commentaires des utilisateurs
    rate = scrapy.Field()          # Note associée au commentaire
           