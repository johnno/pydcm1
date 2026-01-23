pydcm1
======

pydcm1 is a Python library to connect to a Cloud DCM1 Zone Mixer. You can see the current mapping of sources to zones as well as 
request to change the source that a zone is using.

It is primarily being developed with the intent of supporting [home-assistant](https://github.com/home-assistant/home-assistant)


Installation
------------

    # Installing from PyPI
    $ pip install pydcm1

Using on the command line as a script
=====================================


    main.py -h <dcm1 ip address> [-p <port> -s <zone_id> -z <zone_id> -i <source_id> -a -l]
	    -h hostname or ip		Hostname or IP of the DCM1 - required
	    -p port			Port of the DCM1 to connect to - defaults to 23
	    -z zone_id -i source_id	Set zone ID to use source ID - specified as an int e.g. -i 2 -z 4 both must be specified
	    -s zone_id	        Display the source for this zone ID must be an int e.g. 2
	    -a				Display the source for all zones
	    -l				Continue running and listen for source changes

Log out the source used on a specific zone and exit

    python3 main.py -h 192.168.1.100 -s 02

Log out all source/zone mappings and exit

    python3 main.py -h 192.168.1.100 -a

Change zone id 2 to source 3 and exit

    python3 main.py -h 192.168.1.100 -z 2 -i 3

Run forever logging status changes

    python3 main.py -h 192.168.1.100 -l

Change a source then run forever logging status changes

    python3 main.py -h 192.168.1.100 -z 2 -i 3 -l


Using in an application
=======================

See `example.py`
    
    
TODO
=======================

* Get names of sources and zones from the DCM1
