import mariadb
import sys

try:
    con = mariadb.connect(
        user='homeassistant',
        password="xxx",
        host="xxx",
        port=3306,
        database="homeassistant")
except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)

ids_cur = con.cursor()
ids_cur.execute("select id from homeassistant.statistics_meta")
ids = ids_cur.fetchall()

update = con.cursor()
for id in ids:
    # print(id[0])
    statistics_cur = con.cursor()
    statistics_cur.execute("select * from homeassistant.statistics where state IS NOT NULL and metadata_id = " + str(id[0]))
    statistics = statistics_cur.fetchall()

    first = True
    for stat in statistics:
        # print ("input: " + str(stat[3]) + ", " + str(stat[8]) + ", " + str(stat[9]))
        if first:
            previous_state = stat[8]
            previous_sum = stat[9]
            first = False
            # print("first")
        if previous_state > stat[8]:
            new_sum = previous_sum + stat[8]
            # print("reset")
        else:
            new_sum = previous_sum + stat[8] - previous_state
        #     print("normal")
        # print("pstate: " + str(previous_state) + ", psum: " + str(previous_sum) + ", nsum: " + str(new_sum))
        previous_sum = new_sum
        previous_state = stat[8]
        if (round(new_sum, 2) != round(stat[9], 2)):
            print("updating sum from " + str(stat[9]) + " to " + str(new_sum))
            update.execute("update homeassistant.statistics set sum=" + str(new_sum) + " where id=" + str(stat[0]))

con.commit()
con.close()
