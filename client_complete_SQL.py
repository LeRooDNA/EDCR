import datetime
import os
import sys
import time
import zlib

import mysql.connector
import simplejson
import zmq

# deranged octopus

"""
 "  Configuration
"""
# __relayEDDN = 'tcp://eddn-relay.elite-markets.net:9500'
__relayEDDN = 'tcp://eddn.edcd.io:9500'

__timeoutEDDN = 600000

# Set False to listen to production stream
__debugEDDN = False

# Set to False if you do not want verbose logging
# __logVerboseFile = os.path.dirname(__file__) + '/Logs_Verbose_EDDN_%DATE%.htm'
__logVerboseFile = False

# Set to False if you do not want software name logging
__logSoftwareNameFile = os.path.dirname(__file__) + '/Logs_Software_Name_%DATE%.log'
# __logSoftwareNameFile = False

# Set to False if you do not want JSON logging
# __logJSONFile = os.path.dirname(__file__) + '/Logs_JSON_EDDN_%DATE%.log'
__logJSONFile = False

# A sample list of authorised softwares
__authorisedSoftwares = [
    'E:D Market Connector [Windows]',
    'EDDI',
    'EVA [iPad]',
    'ED-IBE (API)',
    'EVA [iPhone]',
    'E:D Market Connector [Mac OS]',
    'EVA [Android]',
    'Elite G19s Companion App',
    'EDCE',
    'EDAPI Trade Dangerous Plugin',
    'EVA [iPod touch]',
    'Elite: Assistant'
]

# function for DB Connection, Open + Close
# function for Cursor, Open + Close


"""
 "  Test code for inserting data:
"""

cnx = mysql.connector.connect(user='TestUser', password='!QA"WS1qa2ws', host='127.0.0.1', port='3306', database='ecdb_monitor')
cursor = cnx.cursor(buffered=True)
delete_software = ("TRUNCATE TABLE authorisedsoftwares")
check_software = ("SELECT softwareName, usageCount, lastUsed FROM authorisedSoftwares WHERE softwareName = %s")
add_software = ("INSERT INTO authorisedSoftwares (softwareName, usageCount, lastUsed, firstAdded) VALUES (%s, %s, %s, %s)")


for __softwareName in __authorisedSoftwares:
    cursor.execute(check_software, (__softwareName,))
    if cursor.rowcount == 0:
        timeAdded = datetime.datetime.utcnow()
        #no data returned - software doesn't exist so add.
        cursor.execute(add_software, (__softwareName, 0, timeAdded, timeAdded))

cnx.commit()
cursor.close()
cnx.close()

"""
 "  End test code
"""


# Used this to excludes yourself for example has you don't want to handle your own messages ^^
__excludedSoftwares = []

"""
 "  Start
"""


def date(__format):
    d = datetime.datetime.utcnow()
    return d.strftime(__format)


__oldTime = False


def echolog(__str):
    global __oldTime, __logVerboseFile

    __logVerboseFileParsed = False

    if __logVerboseFile is not False:
        __logVerboseFileParsed = __logVerboseFile.replace('%DATE%', str(date('%Y-%m-%d')))

    if (__logVerboseFile is not False) and (not os.path.exists(__logVerboseFileParsed)):
        f = open(__logVerboseFileParsed, 'w')
        f.write(
            '<style type="text/css">html { white-space: pre; font-family: Courier New,Courier,Lucida Sans Typewriter,Lucida Typewriter,monospace; }</style>')
        f.close()

    if (__oldTime is False) or (__oldTime != date('%H:%M:%S')):
        __oldTime = date('%H:%M:%S')
        __str = str(__oldTime) + ' | ' + str(__str)
    else:
        __str = '        ' + ' | ' + str(__str)

    print(__str)
    sys.stdout.flush()

    if __logVerboseFile is not False:
        f = open(__logVerboseFileParsed, 'a')
        f.write(__str + '\n')
        f.close()


def savesoftwarename(__str):
    global __authorisedSoftwares

    if __str not in __authorisedSoftwares:
        __authorisedSoftwares.append(__str)
        __logSoftwareNameFileParsed = False
        if __logSoftwareNameFile is not False:
            __logSoftwareNameFileParsed = __logSoftwareNameFile.replace('%DATE%', str(date('%Y-%m-%d')))

        if __logSoftwareNameFile is not False:
            f = open(__logSoftwareNameFileParsed, 'w')
            for __softwareName in range(len(__authorisedSoftwares)):
                if __softwareName == (len(__authorisedSoftwares) - 1):
                    f.write('\'' + __authorisedSoftwares[__softwareName] + '\'\n')
                else:
                    f.write('\'' + __authorisedSoftwares[__softwareName] + '\',\n')
            f.close()


def echologjson(__json):
    global __logJSONFile

    if __logJSONFile is not False:
        __logJSONFileParsed = __logJSONFile.replace('%DATE%', str(date('%Y-%m-%d')))

        f = open(__logJSONFileParsed, 'a')
        f.write(str(__json) + '\n')
        f.close()


# noinspection PyTypeChecker
def main():
    echolog('Starting EDDN Subscriber')
    echolog('')

    context = zmq.Context.instance()
    subscriber = context.socket(zmq.SUB)

    subscriber.setsockopt(zmq.SUBSCRIBE, b"")
    subscriber.setsockopt(zmq.RCVTIMEO, __timeoutEDDN)

    while True:
        try:
            subscriber.connect(__relayEDDN)
            echolog('Connect to ' + __relayEDDN)
            echolog('')
            echolog('')

            while True:
                __message = subscriber.recv()

                if not __message:
                    subscriber.disconnect(__relayEDDN)
                    echolog('Disconnect from ' + __relayEDDN)
                    echolog('')
                    echolog('')
                    break

                __message = zlib.decompress(__message)
                __json = simplejson.loads(__message)
                __converted = False

                # Handle commodity v1
                if __json['$schemaRef'] == 'https://eddn.edcd.io/schemas/commodity/1' + ('/test' if (__debugEDDN is True) else ''):
                    echologjson(__message)
                    echolog('Receiving commodity-v1 message...')
                    echolog('    - Converting to v2...')

                    __temp = {'$schemaRef': 'https://eddn.edcd.io/schemas/commodity/2' + ('/test' if (__debugEDDN is True) else ''), 'header': __json['header'], 'message': {}}

                    __temp['message']['systemName'] = __json['message']['systemName']
                    __temp['message']['stationName'] = __json['message']['stationName']
                    __temp['message']['timestamp'] = __json['message']['timestamp']

                    __temp['message']['commodities'] = []

                    __commodity = {}

                    if 'itemName' in __json['message']:
                        __commodity['name'] = __json['message']['itemName']

                    if 'buyPrice' in __json['message']:
                        __commodity['buyPrice'] = __json['message']['buyPrice']
                    if 'stationStock' in __json['message']:
                        __commodity['supply'] = __json['message']['stationStock']
                    if 'supplyLevel' in __json['message']:
                        __commodity['supplyLevel'] = __json['message']['supplyLevel']

                    if 'sellPrice' in __json['message']:
                        __commodity['sellPrice'] = __json['message']['sellPrice']
                    if 'demand' in __json['message']:
                        __commodity['demand'] = __json['message']['demand']
                    if 'demandLevel' in __json['message']:
                        __commodity['demandLevel'] = __json['message']['demandLevel']

                    __temp['message']['commodities'].append(__commodity)
                    __json = __temp
                    del __temp, __commodity

                    __converted = True

                # Handle commodity v2
                if __json['$schemaRef'] == 'https://eddn.edcd.io/schemas/commodity/2' + ('/test' if (__debugEDDN is True) else ''):
                    if __converted is False:
                        echologjson(__message)
                        echolog('Receiving commodity-v2 message...')

                    __authorised = False
                    __excluded = False

                    if __json['header']['softwareName'] in __authorisedSoftwares:
                        __authorised = True
                    if __json['header']['softwareName'] in __excludedSoftwares:
                        __excluded = True

                    echolog('    - Software: ' + __json['header']['softwareName'] + ' / ' + __json['header']['softwareVersion'])
                    echolog('        - ' + 'AUTHORISED' if (__authorised is True) else
                            ('EXCLUDED' if (__excluded is True) else 'UNAUTHORISED')
                            )

                    if __authorised is True and __excluded is False:
                        # Do what you want with the data...
                        # Have fun !

                        # For example
                        echolog('    - Timestamp: ' + __json['message']['timestamp'])
                        echolog('    - Uploader ID: ' + __json['header']['uploaderID'])
                        echolog('        - System Name: ' + __json['message']['systemName'])
                        echolog('        - Station Name: ' + __json['message']['stationName'])

                        for __commodity in __json['message']['commodities']:
                            echolog('            - Name: ' + str(__commodity['name']))
                            echolog('                - Buy Price: ' + str(__commodity['buyPrice']))
                            echolog('                - Supply: ' + str(__commodity['supply'])
                                    + (
                                        (' (' + __commodity[
                                            'supplyLevel'] + ')') if 'supplyLevel' in __commodity else '')
                                    )
                            echolog('                - Sell Price: ' + str(__commodity['sellPrice']))
                            echolog('                - Demand: ' + str(__commodity['demand'])
                                    + (
                                        (' (' + __commodity[
                                            'demandLevel'] + ')') if 'demandLevel' in __commodity else '')
                                    )
                            # End example

                    del __authorised, __excluded

                    echolog('')
                    echolog('')

                # Handle commodity v3
                if __json['$schemaRef'] == 'https://eddn.edcd.io/schemas/commodity/3' + ('/test' if (__debugEDDN is True) else ''):
                    if __converted is False:
                        echologjson(__message)
                        echolog('Receiving commodity-v3 message...')

                    __authorised = False
                    __excluded = False

                    savesoftwarename(__json['header']['softwareName'])

                    if __json['header']['softwareName'] in __authorisedSoftwares:
                        __authorised = True
                    if __json['header']['softwareName'] in __excludedSoftwares:
                        __excluded = True

                    echolog('    - Software: ' + __json['header']['softwareName'] + ' / ' + __json['header']['softwareVersion'])
                    echolog('        - ' + 'AUTHORISED' if (__authorised is True) else
                            ('EXCLUDED' if (__excluded is True) else 'UNAUTHORISED')
                            )

                    if __authorised is True and __excluded is False:
                        # Do what you want with the data...
                        # Have fun !

                        # For example
                        echolog('    - Timestamp: ' + __json['message']['timestamp'])
                        echolog('    - Uploader ID: ' + __json['header']['uploaderID'])
                        echolog('        - System Name: ' + __json['message']['systemName'])
                        echolog('        - Station Name: ' + __json['message']['stationName'])

                        for __commodity in __json['message']['commodities']:
                            echolog('            - Name: ' + __commodity['name'])
                            echolog('                - Buy Price: ' + str(__commodity['buyPrice']))
                            echolog('                - Supply: ' + str(__commodity['stock'])
                                    + (
                                        (' (' + __commodity[
                                            'supplyLevel'] + ')') if 'supplyLevel' in __commodity else '')
                                    )
                            echolog('                - Sell Price: ' + str(__commodity['sellPrice']))
                            echolog('                - Demand: ' + str(__commodity['demand'])
                                    + (
                                        (' (' + str(__commodity[
                                                        'demandBracket']) + ')') if 'demandBracket' in __commodity else '')
                                    )
                            # End example

                    del __authorised, __excluded

                    echolog('')
                    echolog('')

                del __converted

        except zmq.ZMQError as e:
            echolog('')
            echolog('ZMQSocketException: ' + str(e))
            subscriber.disconnect(__relayEDDN)
            echolog('Disconnect from ' + __relayEDDN)
            echolog('')
            time.sleep(5)


if __name__ == '__main__':
    main()
