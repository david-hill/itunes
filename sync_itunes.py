#!/usr/bin/python
import re
import MySQLdb
import sys  
import musicbrainzngs
import time
from html.entities import name2codepoint as n2cp
#reload(sys)  
#sys.setdefaultencoding('utf8')
debug=0
tot=0
db = MySQLdb.connect("192.168.1.3","musicbrainz","musicbrainz","musicbrainz" )
db.set_character_set('utf8')
c = db.cursor()
c.execute('SET character_set_connection=utf8;')
c.execute("SET NAMES utf8;")
c.execute("SET CHARACTER SET utf8;")

file = open('Library.xml')
fields = [ 'Artist', 'Location', 'Album', 'Year' ]
dict = {'inc':0}
artists = {}
def decodeHtmlentities(string):
    if debug > 1:
      print("Entering decodeHtmlentities")
    rc=-1
    try:
      entity_re = re.compile("&(#?)(\d{1,5}|\w{1,8});")
      def substitute_entity(match):
          ent = match.group(2)
          if match.group(1) == "#":
              return chr(int(ent))
          else:
              cp = n2cp.get(ent)
              if cp:
                  return chr(cp)
              else:
                  return match.group()
      rc=entity_re.subn(substitute_entity, string)[0]
    except:
      print("%s",sys.exc_info())
      pass
    if debug > 1:
      print("Exiting decodeHtmlentities")
    return rc
def count_itunes_locations():
  tot=0
#  return 1
  if debug:
    start = time.time()
    end = 0
    sys.stdout.write("DEBUG: Counting files in iTunes library ... ")
    sys.stdout.flush()
  for line in file:
    rs="<key>Location</key><.*>(.*)</.*>"
    m = re.search(rs, line)
    if m:
      tot+=1
  if debug:
    end = time.time() - start
    sys.stdout.write("done in %ds\n" % (end))
    sys.stdout.flush()
  return tot
def extract_from_itunes():
  print("Extracting " + str(nbr_loc) + " songs...")
  album='empty'
  artist='empty'
  partist='empty'
  pyear=0
  year=0
  inc=0
  prog=0
  eta=0
  file.seek(0)
  estart = time.time()
  for line in file:
#    if prog > 1000:
#      break
    if debug > 1:
      print("%s" % line)
    for field in fields:
      rs="<key>" + field + "</key><.*>(.*)</.*>"
      ctime = time.time()
      m = re.search(rs, line)
      if m:
        if field is not 'Location':
          dict.update({field:m.group(1)})
        if field is 'Location':
          prog+=1 
          sys.stdout.write("\rProgress %.2f%s [%d/%d] Running: %ds ETA: %ds" % (float(prog) / float(nbr_loc) * 100, '%', prog, nbr_loc, ctime - estart,  eta  ) )
          sys.stdout.flush()
          for f in fields:
            if f is not 'Location':
              try:
                if debug:
                  print("dict: %s" % dict[f])
              except:
                pass
          pdict=dict
          partist=artist
          palbum=album
          pyear=year
          artist=str(decodeHtmlentities(dict['Artist'])).lower()
          album=str(decodeHtmlentities(dict['Album'])).lower()
          print("artist: %s, album: %s" % ( artist, album ) )
          try:
            year=decodeHtmlentities(dict['Year'])
          except:
            year='0'
          if (partist != artist or palbum != album):
            eta=int( float(ctime - estart) / float(prog) * (nbr_loc - prog) )
            for p in artists.keys():
              if p == partist:
                if palbum not in artists[ partist ].keys():
                  artists[ partist ][ palbum ]=pdict.copy()
                else:
                  artists[ partist ][ palbum ]['inc'] += inc
            if partist not in artists.keys():
              artists[ partist ] = {}
              artists[ partist ][ palbum ]=pdict.copy()
            dict.update({'inc': 1})
          else:
            inc=dict['inc'] + 1
            dict.update({'inc': inc})
                
tstart = time.time()

nbr_loc=count_itunes_locations()
extract_from_itunes()

prog=0
estart = time.time()

print("INFO: Updating database")
for artist in artists:
  if artist == 'empty':
    continue
  if debug:
    print("Artist: %s" % artist)
  for album in artists[artist]:
    if debug:
      print("Album: %s" % album)
    if artists[artist][album]['inc']:
      ctime = time.time()
      prog+=artists[artist][album]['inc']
      eta=int( float(ctime - estart) / float(prog) * (nbr_loc - prog) )
      sys.stdout.write("\rProgress %.2f%s [%d/%d] Running: %ds ETA: %ds" % (float(prog) / float(nbr_loc) * 100, '%', prog, nbr_loc, ctime - estart,  eta  ) )
      if debug:
        print("Count: %s" % artists[artist][album]['inc'])
      if (artist != "unknown" and album != "unknown"):
        eartist=MySQLdb.escape_string(artist).decode()
        ealbum=MySQLdb.escape_string(album).decode()
        sql = "select present from musicbrainz where artist like '" + eartist + "' and name like '" + ealbum  +"';"
        if debug:
          print("sql: %s" % sql)
        r=c.execute(sql)
        if c.rowcount:
          (result,)=c.fetchone()
          if (result != artists[ artist ][ album ]['inc']):
            sys.stdout.write("\n")
            if debug:
              print("WARNING: %d != %d and corrected to %d for artist %s and album %s" % ( result, artists[ artist ][ album ]['inc'], artists[ artist ][ album ]['inc'], eartist, ealbum ))
            sql = "update musicbrainz set present=" + str(artists[ artist ][ album ]['inc']) + " where artist like '" + eartist + "' and name like '" + ealbum  +"';"
            if debug:
              sys.stdout.write("\n")
              print("sql: %s" % sql)
            sys.stdout.flush()
            r=c.execute(sql)
            rall = c.fetchall()
        else:
          if (album != 'empty' and artist != 'empty'):
            etype="Album"
            eyear=artists[artist][album]['Year']
            sql = "insert into musicbrainz values(" + str(artists[ artist ][ album ]['inc']) + ", '" + eartist + "','" + ealbum + "','" + etype + "','" + eyear + "',NULL);"
            if debug:
              sys.stdout.write("\n")
              print("sql: %s" % sql)
              print("INFO: New album found = Count: %d Artist: %s Album: %s Year: %d" % ( artists[ artist ][ album ]['inc'], eartist, ealbum, int(eyear) ))
            try:
              sys.stdout.flush()
              r=c.execute(sql)
              rall = c.fetchall()
            except:
              print("%s", sys.exc_info())
              pass

tend = time.time() - tstart
sys.stdout.write("\n\ndone in %ds\n" % (tend))
sys.stdout.flush()
