import scrapy

class MovieSpider(scrapy.Spider):
    name = 'movies'
    data = dict()
    movies_ids = dict()
    start_urls = ['https://www.metacritic.com/browse/movies/score/metascore/all/filtered?view=detailed']

    def parse(self, response):
        # map each movie title with the metacritic ranking# (unique) as in {title : id}
        self.movies_ids.update(zip(response.css("span.title.numbered + a h3::text").getall(),
                                            [int(x.strip(' .\n')) for x in response.css("span.title.numbered::text").getall()]))
        for link in response.css('span.title.numbered + a::attr(href)'):
            yield response.follow(link.get(), callback=self.parse_items)

            next_page = response.css('span.next a.action::attr(href)').get()
            if next_page is not None:
                    yield response.follow(next_page, callback=self.parse)
    
    def parse_items(self, response):
        # cert_rating = response.css('.details_section .cert_rating::text').get()
        cert_rating = response.css('.rating span+span::text').get()

        info = {
            'id' : None,
            'title' : response.css('div.product_page_title h1::text').get(), 
            'distributor' : response.css('.details_section .distributor a::text').get(),
            'date' : response.css('.details_section .release_date span + span::text').get(),
            'starring' : response.css('.summary_cast.details_section a::text').getall(),
            'summary' : response.css("div.summary_deck.details_section .blurb.blurb_expanded::text").get() 
                                        if response.css("div.summary_deck.details_section .blurb.blurb_expanded").get() # expanded summary
                                        else response.css('div.summary_deck.details_section span span::text').get(),   # short summary
            'director': response.css('.director a span::text').getall(),
            'genres' : response.css('.genres span span::text').getall(),
            'rating' : cert_rating.strip() if cert_rating else 'Not Rated',    # check if the movie contains a rating
            'runtime' : response.css('.runtime span+ ::text').get(),
            'awards' : response.css(".ranking_title a::text").getall() if response.css(".module.list_rankings") else [],     # get awards & rankings list if exists            
            'stream_link' : response.css('div.details_section a.esite_url::attr(href)').getall(),
            'image' : response.css('img.summary_img::attr(src)').get()
        }
        info.update(id=self.movies_ids.pop(info.get('title'), -1))   # assign the ranking number as the id for each movie

        metaScore = {
            'score' : response.css('.distribution a .metascore_w::text').get(),
            'distri' : {
                'positive':  response.css('.distribution .chart.positive .count.fr::text').get(),
                'mixed': response.css('.distribution .chart.mixed .count.fr::text').get(),
                'negative':  response.css('.distribution .chart.negative .count.fr::text').get(),
            },
            'review_link': response.urljoin(response.css('.distribution a.metascore_anchor::attr(href)').get()) # crtic-reviews link
        }

        userScore = {
            'score' : response.css('.distribution a .metascore_w::text')[1].get(),
            'distri' : {
                'positive': response.css('.distribution .chart.positive .count.fr::text')[1].get(),
                'mixed': response.css('.distribution .chart.mixed .count.fr::text')[1].get(),
                'negative':  response.css('.distribution .chart.negative .count.fr::text')[1].get(),
            },
            'review_link': response.urljoin(response.css('.distribution a.metascore_anchor::attr(href)')[1].get()) # user-reviews link
        }
        
        self.data.update(
            info,
            meta_score = metaScore,
            user_score = userScore,
        )

        yield self.data
    
