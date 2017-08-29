import scrapy


class MicroArea(scrapy.Item):
    distrito_sanitario = scrapy.Field()
    centro_saude = scrapy.Field()
    year = scrapy.Field()

    codigo_micro_area = scrapy.Field()
    residentes = scrapy.Field()
    homens_residentes = scrapy.Field()
    mulheres_residentes = scrapy.Field()

    # residentes por idade (anos)
    menos_1_ano = scrapy.Field()
    anos_1_a_4 = scrapy.Field()
    anos_5 = scrapy.Field()
    anos_6_a_9 = scrapy.Field()
    anos_10_a_19 = scrapy.Field()
    anos_20_a_24 = scrapy.Field()
    anos_25_49 = scrapy.Field()
    anos_50_a_59 = scrapy.Field()
    anos_60_a_64 = scrapy.Field()
    anos_65_a_69 = scrapy.Field()
    anos_acima_70 = scrapy.Field()

    # homens residentes por idade (anos)
    homens_menos_1_ano = scrapy.Field()
    homens_anos_1_a_4 = scrapy.Field()
    homens_anos_5 = scrapy.Field()
    homens_anos_6_a_9 = scrapy.Field()
    homens_anos_10_a_19 = scrapy.Field()
    homens_anos_20_a_24 = scrapy.Field()
    homens_anos_25_49 = scrapy.Field()
    homens_anos_50_a_59 = scrapy.Field()
    homens_anos_60_a_64 = scrapy.Field()
    homens_anos_65_a_69 = scrapy.Field()
    homens_anos_acima_70 = scrapy.Field()

    # mulheres residentes por idade (anos)
    mulheres_menos_1_ano = scrapy.Field()
    mulheres_anos_1_a_4 = scrapy.Field()
    mulheres_anos_5 = scrapy.Field()
    mulheres_anos_6_a_9 = scrapy.Field()
    mulheres_anos_10_a_19 = scrapy.Field()
    mulheres_anos_20_a_24 = scrapy.Field()
    mulheres_anos_25_49 = scrapy.Field()
    mulheres_anos_50_a_59 = scrapy.Field()
    mulheres_anos_60_a_64 = scrapy.Field()
    mulheres_anos_65_a_69 = scrapy.Field()
    mulheres_anos_acima_70 = scrapy.Field()


class CrawlerSaudePMF(scrapy.Spider):
    name = "saude_pmf"

    custom_settings = {
        'HTTPCACHE_ENABLED': True
    }

    # é possível trocar o método start_requests pelo atributo 
    # de classe start_urls = []

    def start_requests(self):
        urls = [
            'http://www.pmf.sc.gov.br/sistemas/saude/unidades_saude/populacao/uls_2015_index.php'
        ]

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        """
        """

        year = self.get_year(response.url)

        # Get all tables from page
        tables = response.css('table')
        # Get lines from table 1. Table 1 has the needed data
        table_trs = tables[1].css('tr')
        micro_areas = []
        for tr in table_trs:
            micro_areas.append(self.get_links_micro_areas(tr.css('td'), year))

        for info in micro_areas:
            micro_areas = info['micro_areas']
            for area in micro_areas:
                self.log('Area: {}'.format(area))
                partial_link = area['link']
                full_link = 'http://www.pmf.sc.gov.br/sistemas/saude/unidades_saude/populacao/' + partial_link

                self.log('Full link: {}'.format(full_link))
                yield scrapy.Request(url=full_link,
                                     callback=self.parse_micro_areas,
                                     meta={
                                            'ds': info['distrito_sanitario'],
                                            'cs': info['centro_saude'],
                                            'year': info['year']
                                          })

    def get_links_micro_areas(self, tds, year):
        distrito_sanitario = tds[0].css('a::text').extract_first()
        centro_saude = tds[1].css('a::text').extract_first()

        self.log('Distrito Sanitário: {}'.format(distrito_sanitario))
        self.log('Centro de Saúde {}'.format(centro_saude))

        micro_areas = []
        links = tds[2].css('a')
        self.log('Num links: {}'.format(len(links)))
        for link in links:
            # links.append(td.css('a'))
            micro_area_name = link.css('a::text').extract_first()
            micro_area_link = link.css('a::attr(href)').extract_first()
            self.log('micro area name: {}'.format(micro_area_name))
            self.log('micro area link: {}'.format(micro_area_link))

            micro_area = {}
            micro_area['name'] = micro_area_name
            micro_area['link'] = micro_area_link
            micro_areas.append(micro_area)

        entry = {}
        entry['distrito_sanitario'] = distrito_sanitario
        entry['centro_saude'] = centro_saude
        entry['micro_areas'] = micro_areas
        entry['year'] = year
        return entry

    def get_year(self, url):
        """Get the year from url.

        Arguments:
        url -- link to remove the year
        """
        last_part = url.split('/')[-1]
        begin = last_part.find('_')
        end = last_part.rfind('_')
        return last_part[begin + 1:end]

    def parse_micro_areas(self, response):
        """Form a complex number.

        Keyword arguments:
        real -- the real part (default 0.0)
        imag -- the imaginary part (default 0.0)
        """
        table = response.css('table')
        trs = table.css('tr')

        distrito_sanitario = response.meta['ds']
        centro_saude = response.meta['cs']
        year = response.meta['year']

        for tr in trs[2:-1]:
            micro_area = MicroArea()
            data = tr.css('td::text').extract()

            micro_area['distrito_sanitario'] = distrito_sanitario
            micro_area['centro_saude'] = centro_saude
            micro_area['year'] = year
            micro_area['codigo_micro_area'] = data[0]
            micro_area['residentes'] = data[1]
            micro_area['homens_residentes'] = data[2]
            micro_area['mulheres_residentes'] = data[3]

            # residentes por idade (anos)
            micro_area['menos_1_ano'] = data[4]
            micro_area['anos_1_a_4'] = data[5]
            micro_area['anos_5'] = data[6]
            micro_area['anos_6_a_9'] = data[7]
            micro_area['anos_10_a_19'] = data[8]
            micro_area['anos_20_a_24'] = data[9]
            micro_area['anos_25_49'] = data[10]
            micro_area['anos_50_a_59'] = data[11]
            micro_area['anos_60_a_64'] = data[12]
            micro_area['anos_65_a_69'] = data[13]
            micro_area['anos_acima_70'] = data[14]

            # homens residentes por idade (anos)
            micro_area['homens_menos_1_ano'] = data[15]
            micro_area['homens_anos_1_a_4'] = data[16]
            micro_area['homens_anos_5'] = data[17]
            micro_area['homens_anos_6_a_9'] = data[18]
            micro_area['homens_anos_10_a_19'] = data[19]
            micro_area['homens_anos_20_a_24'] = data[20]
            micro_area['homens_anos_25_49'] = data[21]
            micro_area['homens_anos_50_a_59'] = data[22]
            micro_area['homens_anos_60_a_64'] = data[23]
            micro_area['homens_anos_65_a_69'] = data[24]
            micro_area['homens_anos_acima_70'] = data[25]

            # mulheres residentes por idade (anos)
            micro_area['mulheres_menos_1_ano'] = data[26]
            micro_area['mulheres_anos_1_a_4'] = data[27]
            micro_area['mulheres_anos_5'] = data[28]
            micro_area['mulheres_anos_6_a_9'] = data[29]
            micro_area['mulheres_anos_10_a_19'] = data[30]
            micro_area['mulheres_anos_20_a_24'] = data[31]
            micro_area['mulheres_anos_25_49'] = data[32]
            micro_area['mulheres_anos_50_a_59'] = data[33]
            micro_area['mulheres_anos_60_a_64'] = data[34]
            micro_area['mulheres_anos_65_a_69'] = data[35]
            micro_area['mulheres_anos_acima_70'] = data[36]

            yield micro_area
