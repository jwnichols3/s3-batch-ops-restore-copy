import time

copy_start_time_tup = time.localtime()  # get struct_time
copy_start_time_str = time.strftime(
    "%Y-%m-%d, %H:%M:%S", copy_start_time_tup)

print("Two step time: " + copy_start_time_str)

onestep = time.strftime("%Y-%m-%d, %H:%M:%S", time.localtime())

print("One step time: " + onestep)

print("Starting at " + time.strftime("%Y-%m-%d, %H:%M:%S", time.localtime()))

print(time.localtime())

object = "AAAA"
print(time.strftime("%Y-%m-%d, %H:%M:%S",
                    time.localtime()) + " - " + object + " - start")


now = time.time()
print("Start: " + time.strftime("%Y-%m-%d, %H:%M:%S", time.localtime()))
time.sleep(2)
later = time.time()
print("End: " + time.strftime("%Y-%m-%d, %H:%M:%S", time.localtime()))
difference = int(later - now)
print("Difference: " + str(difference))

print
