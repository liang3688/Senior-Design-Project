import scrapy

class movies_crawler(scrapy.Spider):
    name = 'movie_reviews'
    allowed_domains = ['metacritic.com']
    #start_urls = ['https://www.metacritic.com/browse/movies/score/metascore/all/filtered?sort=desc&page=34']
    start_urls = ['https://www.metacritic.com/browse/movies/score/metascore/all/filtered?sort=desc']
    DOWNLOAD_DELAY = 1.5

    def parse(self, response):
        for link in response.css('div.title_bump div.browse_list_wrapper table.clamp-list td.clamp-image-wrap a::attr(href)'):
            yield response.follow(link.get(), callback=self.parse_movie)
            
        next_page = response.css('span.next a.action::attr(href)').get()
        if next_page is not None:
            next_page = response.urljoin(next_page)
            yield scrapy.Request(next_page, callback=self.parse)
         
    def parse_movie(self, response):
        for critic_review_link in response.xpath('//*[@id="nav_to_metascore"]/div[1]/div[5]/a/@href'):
            yield response.follow(critic_review_link.get(), callback=self.critic_review)
            
        for user_review_link in response.xpath('//*[@id="nav_to_metascore"]/div[2]/div[5]/a/@href'):
            yield response.follow(user_review_link.get(), callback=self.user_review)
            
    
    def critic_review(self, response):
        for products in response.css('div.review.pad_top1.pad_btm1'):
            if products.css('div.review.pad_top1 div.right.fl div.summary a.no_hover::text').get() is not None:
                yield {
                    'review_type': "Critic Review",
                    'release_date': response.css('table.simple_summary td.left span.release_date span + span::text').get(),
                    'movie_name': response.css('div.next_to_side_col div.product_page_title a h1::text').get(),
                    'meta_score': response.css('table.simple_summary td.num_wrapper span::text').get(),
                    'review_score': products.css('div.left.fl div.metascore_w::text').get(),
                    'review': products.css('div.right.fl div.summary a.no_hover::text').get().strip()        
                }
            
            
    def user_review(self, response):
                
        for products in response.css('div.review.pad_top1'):
            if products.css('div.review.pad_top1 div.right div.title.pad_btm_half span.author::text').get() is not None and products.css('div.review.pad_top1 div.right div.summary div.review_body span.inline_expand_collapse span.blurb_expanded').get() is not None:
                yield {
                    'review_type': 'User Review',
                    'release_date': response.css('div.next_to_side_col table.simple_summary td.left span.release_date span + span::text').get(),
                    'movie_name': response.css('div.next_to_side_col div.product_page_title a h1::text').get(),
                    'user_review_score': response.css('div.next_to_side_col table.simple_summary td.num_wrapper span::text').get(),
                    'reviewer_name': products.css('div.review.pad_top1 div.right div.title.pad_btm_half span.author::text').get(),
                    'review_score': products.css('div.review.pad_top1 div.left div.metascore_w::text').get(),
                    'review': products.css('div.review.pad_top1 div.right div.summary div.review_body span.inline_expand_collapse span.blurb_expanded').get().replace('</span>', '').replace('<span class="blurb blurb_expanded">', '').replace('<br>', '').strip()
                }
            elif products.css('div.review.pad_top1 div.right div.title.pad_btm_half span.author::text').get() is not None and products.css('div.review.pad_top1 div.right div.summary div.review_body span.inline_expand_collapse span.blurb_expanded').get() is None:
                yield {
                    'review_type': 'User Review',
                    'release_date': response.css('div.next_to_side_col table.simple_summary td.left span.release_date span + span::text').get(),
                    'movie_name': response.css('div.next_to_side_col div.product_page_title a h1::text').get(),
                    'user_review_score': response.css('div.next_to_side_col table.simple_summary td.num_wrapper span::text').get(),
                    'reviewer_name': products.css('div.review.pad_top1 div.right div.title.pad_btm_half span.author::text').get(),
                    'review_score': products.css('div.review.pad_top1 div.left div.metascore_w::text').get(),
                    'review': products.css('div.review.pad_top1 div.right div.summary div.review_body span::text').get().strip()
            }
            elif products.css('div.review.pad_top1 div.right div.title.pad_btm_half span.author a::text').get() is not None and products.css('div.review.pad_top1 div.right div.summary div.review_body span.inline_expand_collapse span.blurb_expanded').get() is not None:
                 yield {
                    'review_type': 'User Review',
                    'release_date': response.css('div.next_to_side_col table.simple_summary td.left span.release_date span + span::text').get(),
                    'movie_name': response.css('div.next_to_side_col div.product_page_title a h1::text').get(),
                    'user_review_score': response.css('div.next_to_side_col table.simple_summary td.num_wrapper span::text').get(),
                    'reviewer_name': products.css('div.review.pad_top1 div.right div.title.pad_btm_half span.author a::text').get(),
                    'review_score': products.css('div.review.pad_top1 div.left div.metascore_w::text').get(),
                    'review': products.css('div.review.pad_top1 div.right div.summary div.review_body span.inline_expand_collapse span.blurb_expanded').get().replace('</span>', '').replace('<span class="blurb blurb_expanded">', '').replace('<br>', '').strip()
                }
           
            elif products.css('div.review.pad_top1 div.right div.title.pad_btm_half span.author a::text').get() is not None and products.css('div.review.pad_top1 div.right div.summary div.review_body span.inline_expand_collapse span.blurb_expanded').get() is None:
                yield {
                    'review_type': 'User Review',
                    'release_date': response.css('div.next_to_side_col table.simple_summary td.left span.release_date span + span::text').get(),
                    'movie_name': response.css('div.next_to_side_col div.product_page_title a h1::text').get(),
                    'user_review_score': response.css('div.next_to_side_col table.simple_summary td.num_wrapper span::text').get(),
                    'reviewer_name': products.css('div.review.pad_top1 div.right div.title.pad_btm_half span.author a::text').get(),
                    'review_score': products.css('div.review.pad_top1 div.left div.metascore_w::text').get(),
                    'review': products.css('div.review.pad_top1 div.right div.summary div.review_body span::text').get().strip()
            }
                                   
        next_page = response.css('div.page_nav a.action::attr(href)').get()
        if next_page is not None:
            next_page = response.urljoin(next_page)
            yield scrapy.Request(next_page, callback=self.user_review)
          
                
                
   

























                
                
    # def parse_item(self, response):
    #     products = response.css('div.fxdrow')
    #     for product in products:
    #             yield {
                         #   'movie_name': product.css('div.product_page_title h1::text').get(), 
    #                    # 'distributor': product.css('div.fxdcol div.details_section table.cert_rating_wrapper span.distributor a::text').get(),
    #                    # 'date': product.css('div.fxdcol div.details_section table.cert_rating_wrapper span.release_date span::text').getall(),
    #                    # 'cast': product.css('div.fxdcol div.summary_cast.details_section a::text').getall(),
    #                    # 'summary': product.css('div.summary_deck span.blurb_expanded span::text').getall(),
    #                    # 'steam_link': product.css('div.details_section a.esite_url::attr(href)').getall()
    #             }
               
    # def parse(self, response):
    #    for products in response.css('td.clamp-summary-wrap'):
    #         try:
    #             yield {
    #                     'name': products.css('a.title h3::text').get(),
    #                     'date': products.css('div.clamp-details span::text').get(),
    #                     'rating': products.css('div.clamp-details span.cert_rating::text').get().replace(' | ',''),
    #                     'summary': products.css('div.summary::text').get().strip(),
    #                     'meta_score': products.css('div.clamp-metascore div.metascore_w::text').get(),
    #                     'user_score': products.css('div.clamp-userscore div.metascore_w::text').get()
    #             }
    #        except:
    #             yield {
    #                     'name': products.css('a.title h3::text').get(),
    #                     'date': products.css('div.clamp-details span::text').get(),
    #                     'rating': 'N/A',
    #                     'summary': products.css('div.summary::text').get().strip(),
    #                     'meta_score': products.css('div.clamp-metascore div.metascore_w::text').get(),
    #                     'user_score': products.css('div.clamp-userscore div.metascore_w::text').get()
    #             }

                # next_page = response.css('span.next a.action::attr(href)').get()
                # if next_page is not None:
                #     next_page = response.urljoin(next_page)
                #     yield scrapy.Request(next_page, callback=self.parse)