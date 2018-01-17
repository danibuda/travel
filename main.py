from database import connection
from assets import access
from scripts.manager import MapClient, ScrapeManager
from scripts.wp_controller import WordPressManager
from time import gmtime, strftime
import logging

logging_flag = False
root_path = "C:\\Users\\fabbs\\Google Drive\\Mapping Romania"
domains = ["https://www.welcometoromania.ro/Romania/Romania_Harta_Judete_r.htm",
           "https://carta.ro/obiective-turistice-romania/", "http://obiectiveturistice.drumliber.ro/",
           "http://obiective-turistice.romania-tourist.info/obiective-turistice-romania",
           "http://www.cimec.ro/Monumente/LacaseCult/RO/Documente/BazaDate.htm", "https://monumenteuitate.uauim.ro/r/",
           "http://www.monumenteuitate.org/ro/monuments", "http://www.hartis.ro", "http://ghidulmuzeelor.cimec.ro/",
           "http://locuridinromania.ro/", "http://monumente-etnografice.cimec.ro/", "http://www.monumenteromania.ro/",
           "http://www.cimec.ro/Monumente/Zonenaturale.htm", "http://wikimapia.org"]

if __name__ == '__main__':
    access = access.Settings
    log_folder = root_path + "\\_logs\\" + strftime("%d-%m %H-%M-%S", gmtime()) + "_travelatar.log"
    if logging_flag:
        logging.basicConfig(filename=log_folder,
                            filemode="a",
                            format="%(asctime)s - %(levelname)s : %(message)s",
                            datefmt="%H:%M:%S",
                            level=logging.DEBUG)
    maps = MapClient(access, timeout=5, queries_per_second=100, language='ro')
    session = connection.DataBaseView(access)

    manager = ScrapeManager(access, maps, session)
    manager.fix_images(r"C:\Users\fabbs\Desktop\Churches", r"C:\Users\fabbs\Desktop\Test")


    # wp = WordPressManager(access, session)
    # wp.index_taxonomies()
    # wp.index_posts()
    # img = wp.upload_image("C:\\Users\\fabbs\\Desktop\\Churches\\test_2.jpg")

    exit()
