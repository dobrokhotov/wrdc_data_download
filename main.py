from wrdc_data_downloader import wrdc_data_download_hour


station = 'tartu-toravere'
years = [2007, 2020]
rad_data = ['glo', 'dif', 'dir', 'uwl', 'dwl']
out_folder = 'F:/SCIENCE 2023/WRDC/Data/'

wrdc_data_download_hour(station=station, years=years, rad_data=rad_data, out_folder=out_folder)
