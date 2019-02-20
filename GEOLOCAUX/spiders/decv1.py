# -*- coding: utf-8 -*-

#----import packages
import scrapy
import ast
import xmltodict
from datetime import datetime
import time
import re
import random
import  pdb
UTF8_ENCODING = 'utf-8'
import sys
import logging

logger = logging.getLogger()


class GeolocauxSpiderSpider(scrapy.Spider):
    name = "geolocaux"
    allowed_domains = ["geolocaux.com"]
    start_urls = ['https://www.geolocaux.com/sitemap.xml']
    
    def clean_text(self,txt ):
        """Remove line breaks or multiple spaces"""
        txt = txt.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ').replace(';', ' ').replace('"', ' ')  # remov$
        txt = ' '.join(txt.split())  # replace multiple spaces by single one
        return txt

    def cast_float(self,f, decimals=2):
        """Return float with only 2 decimals by default"""        
        if isinstance(f, str):
            # First, clean input if it's a string
            f = f.replace(u'\xa0', '')
            f = f.replace(",", ".")
        f = float(f) if (f is not None) else None
        f = round(f, decimals) if (f is not None) else None
        return f


    def parse(self, response):
        """Parse the sitemap for the different categories (XML) [SITEMAP]"""
        xml = response.text
        sitemap_dict = xmltodict.parse(xml, process_namespaces=False)

        for sitemap in sitemap_dict['sitemapindex']['sitemap']:
            loc = sitemap['loc']
            if "archives" in loc:  # "archive" => don't crawl
                continue
            logger.info('[PARSE_SITEMAP] loc=%s' % loc)
            yield scrapy.Request(
                loc,
                method='GET',
                encoding=UTF8_ENCODING,
                dont_filter=True,  # FILTER!!! Unique sitemap
                callback=self.parse_list,
            )

    def get_request_to_details_page(self, item, referer):

        
        """Utility function. Return request to details page"""
        return scrapy.Request(
            item['ANNONCE_LINK'],
            method='GET',
            encoding=UTF8_ENCODING,
            dont_filter=True,  # FILTER!!! Unique details page
            callback=self.parse_item,
            #errback=lambda failed_response, item=item: self.on_error(failed_response, item),
            meta={
                'item': str(item),
                'dont_redirect': True,
                'details': True
            })

    def parse_list(self, response):
        """Parse the list (XML) [MODELIST]"""
        xml = response.text
        #pdb.set_trace()
        dict = xmltodict.parse(xml, process_namespaces=False)
        #print(dict['urlset']['url'][0])

        for el in dict['urlset']['url'][1:]:  # first one is dummy => don't crawl

            # Check if not a landing page such as
            # https://www.geolocaux.com/bureau/paris-75/
            #if el.has_key('lastmod'):
            if 'lastmod' in el.keys():
                item={}

                # Set last modification date
                last_modification_date = el['lastmod']
                item['ANNONCE_DATE'] = datetime.strptime(last_modification_date, "%Y-%m-%d").strftime(
                    "%Y-%m-%d %H:%M:%S")

                # Get / Set url
                url = el['loc']
                item['ANNONCE_LINK'] = url
                item['SELLER_TYPE'] = 'pro'
  
                # Get information from url
                #pdb.set_trace()
                item['FROM_SITE'] = url.rsplit('/')[2].strip()

                if len(url.rsplit('/')) >= 3 and url.rsplit('/')[3].lower().find('vente') != -1:
                    item['ACHAT_LOC'] = "1"  # vente
                elif len(url.rsplit('/')) >= 3 and url.rsplit('/')[3].lower().find('vendre') != -1:
                    item['ACHAT_LOC'] = "1"  # vente
                elif len(url.rsplit('/')) >= 3 and url.rsplit('/')[3].lower().find('location') != -1:
                    item['ACHAT_LOC'] = "2"  # location
                elif len(url.rsplit('/')) >= 3 and url.rsplit('/')[3].lower().find('louer') != -1:
                    item['ACHAT_LOC'] = "2"  # location
                elif 'location-coworking' in url:
                    item['ACHAT_LOC'] = 2
                elif 'vente' in url or 'vendre' in url or 'achat' in url:
                    item['ACHAT_LOC'] = 1
                elif 'location' in url or 'louer' in url:
                    item['ACHAT_LOC'] = 2
                else:
                    item['ACHAT_LOC'] = ""

                item['CATEGORIE'] = self.clean_text(url.rsplit('/')[4]).lower()
                if 'coworking' in url:
                    item['CATEGORIE'] = 'coworking'  # clean_text(url.rsplit('/')[3]).lower()
                item['ID_CLIENT'] = url.rsplit('/')[-1].split("-")[-1].split('.html')[0]

                # For debug purpose
                item['NEUKOLLN_FROM_LISTING_PAGE'] = response.url

                # Set default phone country here
                item['NEUKOLLN_ORIGINAL_PHONE_AGENCE_TEL'] = 'FR'
                myurl=item['ANNONCE_LINK']
                
                
                yield self.get_request_to_details_page(item, response.url)

    def parse_item(self, response):
        """Parse the details page [MODE DETAILS]"""

        # Retieve item
        self.log("2")
        item = response.meta['item']

        # Convert returned string to dictionary
        item = ast.literal_eval(item)

        ###
        # Extract data
        ###

        # Number of photos
        photos = response.xpath('//div[contains(@class,"photos")]//ul//li')
        print(photos,'y'*10)
        if photos:
            item['PHOTO'] = len(photos)
        if not item.get('PHOTO'):
            photos = response.xpath('//img[contains(@class,"lazy")]')
            if photos:
                photos = photos.extract()
                photos = len(photos) if photos else None
                item['PHOTO'] = photos

        # Description "headpage" => NOM
        description = response.xpath('//div[contains(@class,"headpage")]//h1//text()')
        if description:
            description = description.extract()
            description = [self.clean_text(el) for el in description]  # clean values
            description = list(filter(None, description))  # remove empty values
            item['NOM'] = self.clean_text(" ".join(description))

        # Neuf ou ancien?
        item['NEUF_IND'] = "N"  # ancien
        item['SELLER_TYPE'] = "pro"

        # Adresse

        # cp
        cp = response.xpath('//div[contains(@class,"headpage")]//span[contains(@class,"adr")]//span[@itemprop="postalCode"]//text()')
        if cp:
            cp = cp.extract()
            cp = cp[0] if (cp and len(cp)) else None
            # item['CP'] = int(cast_float(cp, decimals=0))  # I had a zip code like this "L-1736"
            if cp and cp.isdigit():  # I had a zip code like this "L-1736"
                item['CP'] = cp
                item['DEPARTEMENT'] = cp[:2]  # first two digits

        # town
        city = response.xpath('//div[contains(@class,"headpage")]'
                              '//span[contains(@class,"adr")]//span[@itemprop="addressLocality"]//text()')
        if city:
            city = city.extract()
            city = city[0] if (city and len(city)) else None
            item['VILLE'] = clean_text(city)

        # Get the town differently - in the title
        if (not item.get('VILLE') or not item.get("CP")) and item.get('NOM'):
            match = re.search("\d{5}", item['NOM'])

            if match and match.group():
                # there is a zip code
                if not item.get('CP'):
                    cp = match.group()
                    item['CP'] = cp
                    item['DEPARTEMENT'] = cp[:2]  # first two digits

                if item.get('NOM'):
                    ville = re.search("([^(]*) .*", item['NOM'])
                    if ville and ville.groups():
                        try:
                            item['VILLE'] = clean_text(ville.groups()[0].split(" ")[-1])
                        except:
                            pass

        # region
        region = response.xpath('//div[contains(@class,"headpage")]'
                                '//span[contains(@class,"adr")]//meta[@itemprop="addressRegion"]//@content')
        if region:
            region = region.extract()
            region = region[0] if (region and len(region)) else None
            item['REGION'] = clean_text(region)

        # country
        country = response.xpath('//div[contains(@class,"headpage")]'
                                 '//span[contains(@class,"adr")]//meta[@itemprop="addressCountry"]//@content')
        if country:
            country = country.extract()
            country = country[0] if (country and len(country)) else None
            item['PAYS_AD'] = clean_text(country)
        if 'france' in item.get('PAYS_AD', ''):
            item['PAYS_AD'] = 'FR'
            
        # Set default phone country here
        item['NEUKOLLN_DEFAULT_CC_AGENCE_TEL'] = 'FR'

        # description
        description = response.xpath('//div[contains(@class,"cadre_content")]//p//text()')
        if description:
            description = description.extract()
            item['ANNONCE_TEXT'] = self.clean_text(" ".join(description)) if description else None

        # surface
        # if item['CATEGORIE'] == 'terrain':  # FIXME
        surface = response.xpath('//div[contains(@class,"annonce_infos")]//div[contains(@class,"surface")]//text()')
        if surface:
            surface = surface.extract()
            surface = [self.clean_text(el) for el in surface]  # clean values
            surface = list(filter(None, surface))  # remove empty values
            surface = self.clean_text(" ".join(surface))

            m2_totale = re.findall(r'\d*\s*\d+\s*m\xb2', surface)
            if not m2_totale:
                m2_totale = re.findall(r'\d*\s*\d+\s*m2', surface)
            if m2_totale:
                m2_totale = re.findall(r'\d+', m2_totale[0]) if (m2_totale is not None) else None
                m2_totale = "".join(m2_totale) if (m2_totale is not None) else None
                #pdb.set_trace()
                item['M2_TOTALE'] = m2_totale.replace(u'\xa0', '')
                #item['M2_TOTALE'] = self.cast_float(m2_totale, decimals=2)
            else:
                surface = re.findall(r'\d+', surface) if (surface is not None) else None
                surface = "".join(surface) if (surface is not None) else None

                # item['M2_TOTALE'] = self.cast_float(surface, decimals=2)
        #########################
        ville2=re.findall('"addressLocality":"(.*?)"',response.text)
        item['VILLE']=ville2[0]  if ville2 else ''
        pays=re.findall('"addressCountry":{"name":"(.*?)"',response.text)
        item['PAYS_AD']=pays[0] if pays else ''
        #tel=response.xpath('//*[@id="tel"]/text()').extract_first()
        tel = response.xpath('//div[@class="contact"]/div[@class="bottom"]/div[contains(@class,"bloc tel")]/a//@href').extract_first()
        #pdb.set_trace()
        tel =  tel.replace(' ','') if tel else ''
        if tel in ("Contactparemail" , "NC"):
            tel = None
        try:
            tel = tel.replace('(33)','0').replace('\(0\)','').replace('\(1\)','')
        except:
            pass
        try:
            tel = tel.lstrip('33')
        except:
            print('problem in  tel in tel.lstrip("33") ')
        tel =  re.sub('\D', '', tel)
        item['AGENCE_TEL']=tel.replace(u'\xa0', '')
        #########################
        # price
        # pdb.set_trace()
        price = response.xpath('//div[contains(@class,"annonce_infos")]'
                               '//div[contains(@class,"prix")]//span[@id="PriceAnnonce"]//text()')
        if price:
            price = price.extract()
            price = price[0] if (price and len(price)) else None
            price = re.findall('\d+', price) if price else None
            if price:
                price = "".join(price)
                price =  price.replace(u'\xa0', '')

                # total price or price m2
                if price:
                    selected = response.xpath('//div[contains(@class,"value prix")]'
                                              '//div[contains(@class,"selected")]//@class')
                    if selected:
                        selected = selected.extract_first()
                        # possibility:
                        # "it total selected",
                        # "it m2v selected",
                        # "it m2 selected",
                        # "it month selected",
                        # "it year selected",

                        if "total" in selected:
                            item['PRIX'] = price
                        elif "m2v" in selected:
                            item['PRIX'] = int(price) * int( item['M2_TOTALE'] ) if item.get('M2_TOTALE') else None
                        elif "m2" in selected:
                            item['PRIX'] = int(price) * int( item['M2_TOTALE'] ) / float(12) if item.get('M2_TOTALE') else None
                        elif "month" in selected:
                            item['PRIX'] = price
                        elif "year" in selected:
                            item['PRIX'] = price / float(12)
                        item['PRIX'] = self.cast_float(item['PRIX'], decimals=2) if item.get('PRIX') else None
                        item['PRIX'] = int(item['PRIX']) if item.get('PRIX') else None

        # contact

        # mini-site-url
        mini_site_url = response.xpath(
            '//div[@id="Annonceur"]//div[contains(@class,"links")]//a[contains(@class,"biens")]//@href')
        if mini_site_url:
            mini_site_url = mini_site_url.extract()
            mini_site_url = mini_site_url[0] if (mini_site_url and len(mini_site_url)) else None
            item['MINI_SITE_URL'] = "https://www.geolocaux.com%s" % mini_site_url if mini_site_url else None

        # name of the agency
        agency_contact = response.xpath('//div[contains(@class,"annonce_infos")]//div[contains(@class,"contact")]'
                                        '//div[contains(@class,"nom_annonceur")]//text()')
        if agency_contact:
            agency_contact = agency_contact.extract()
            agency_contact = [self.clean_text(el) for el in agency_contact]  # clean values
            agency_contact = list(filter(None, agency_contact))  # remove empty values
            item['AGENCE_CONTACT'] = self.clean_text(" ".join(agency_contact))

        # contact
        agency_name = response.xpath('//div[@id="Annonceur"]//div[contains(@class,"name")]//text()')
        if agency_name:
            agency_name = agency_name.extract_first()
            agency_name = re.search("informations sur l'annonceur : (.*)", agency_name) if agency_name else None
            if agency_name and agency_name.groups():
                item['AGENCE_NOM'] = agency_name.groups()[0]

        # phone
        phone = response.xpath('//div[contains(@class,"tel")]//span[contains(@class,"numero")]//text()')
        if phone:
            phone = phone.extract()
            phone = phone[0] if (phone and len(phone)) else None
            item['NEUKOLLN_ORIGINAL_PHONE_AGENCE_TEL'] = phone

        # get the link to the website of the agency
        more_info_about_agency_url = response.xpath('//div[@id="Annonceur"]//div[contains(@class,"description")]//a//@href')
        if more_info_about_agency_url:
            more_info_about_agency_url = more_info_about_agency_url.extract()
            more_info_about_agency_url = more_info_about_agency_url[0] if (
            more_info_about_agency_url and len(more_info_about_agency_url) == 1) else None
            more_info_about_agency_url = "https://www.geolocaux.com%s" % more_info_about_agency_url if more_info_about_agency_url else None
        if more_info_about_agency_url:
            yield scrapy.Request(
                more_info_about_agency_url,
                headers={
                    "Host": "www.geolocaux.com",
                    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.5",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Connection": "keep-alive",
                    "Referer": "%s" % response.url,
                    "Upgrade-Insecure-Requests": "1",
                    "Cache-Control": "max-age=0",
                },
                method='GET',
                encoding=UTF8_ENCODING,
                dont_filter=True,  # DON'T FILTER! There are several ads for a same agency
                callback=self.parse_more_info_about_agency_page,
                meta={
                    'item': str(item),
                    'neukolln_refresh_cache': False,
                })
        else:
            item['MAISON_APT'] = None
            yield item

    def parse_more_info_about_agency_page(self, response):
        """Retrieve the website information from the page"""

        # Retieve item
        item = response.meta['item']
        self.log("3")
        # Convert returned string to dictionary
        item = ast.literal_eval(item)

        # [DEBUG]
        logger.debug("Retrieve agency website [ID_CLIENT=%s]" % str(item['ID_CLIENT']))

        # Get website
        website = response.xpath('//a[contains(@class,"website")]//text()')
        if website:
            website = website.extract()
            website = website[0] if (website and len(website)) else None

        # Set website
        item['WEBSITE'] = website
        item['MAISON_APT'] = None
        yield item

