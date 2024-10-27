import scrapy
from ..items import FilmscraperItem  # Ensure this import is correct

class FilmspiderSpider(scrapy.Spider):
    name = "filmspider"
    allowed_domains = ["allocine.fr"]
    start_urls = ["https://www.allocine.fr/films/"]

    def parse(self, response):
        # Select the film elements
        films = response.css('li.mdl')
        for film in films:
            bookItem = FilmscraperItem()

            # Extracting data from the film page
            bookItem["url"] = response.url
            title = film.xpath('.//h2[@class="meta-title"]/a/text()').get()
            bookItem["title"] = title.strip() if title else None  # Clean whitespace
            
            duration = film.xpath('.//span[@class="spacer"]/following-sibling::text()[1]').get()
            bookItem['duree'] = duration.strip() if duration else 'Duration not found'
            
            # Extracting rating information safely
            press_rate = film.css('.rating-item .stareval-note::text').getall()
            bookItem['press_rate'] = press_rate[0].strip() if len(press_rate) > 0 else None
            bookItem['spectateur_rate'] = press_rate[1].strip() if len(press_rate) > 1 else None
            
            # Extracting the story description
            bookItem['histoire'] = film.css('.content-txt::text').get().strip() if film.css('.content-txt::text').get() else None
            
            if all(bookItem.values()):  # Check if all values in bookItem are not None
                yield bookItem






   
   
   


        # Extracting the "Suivante" (Next) link for pagination
        next_page = response.css('.gd-col-middle .pagination a::attr(href)').get()
        if next_page:  # Check if next_page exists
            total_pages = 3000  # Set this to the total number of pages you want to scrape
            for page in range(2, total_pages + 1):  # Start from page 2 to the total number of pages
                next_page_url = f"https://www.allocine.fr/films/?page={page}"
                yield scrapy.Request(next_page_url, callback=self.parse)




    """def parse_co(self, response):
        comments = response.css('div.cf')  # Corrected spelling
        for comment in comments:
            bookItem = FilmscraperItem()  # Corrected item type
            bookItem["url"] = response.url
            bookItem["commentaire"] = comment.css('.content-txt.review-card-content::text').get().strip() if comment.css('.content-txt.review-card-content::text').get() else None
            bookItem['rate'] = comment.css('.stareval-note::text').get().strip() if comment.css('.stareval-note::text').get() else None
            
            if all(bookItem.values()):  # Check if all values in bookItem are not None
                yield bookItem

        next_page = response.css('.gd-col-middle .pagination a::attr(href)').get()
        if next_page:  # Check if next_page exists
            total_pages = 40  # Set this to the total number of pages you want to scrape
            for page in range(2, total_pages + 1):  # Start from page 2 to the total number of pages
                next_page_url = f"{response.url}?page={page}"  # Fixed next page URL
                yield scrapy.Request(next_page_url, callback=self.parse_co)  # Fixed callback"""
