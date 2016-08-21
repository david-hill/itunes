import re
import MySQLdb
import sys  
import musicbrainzngs
import time
from htmlentitydefs import name2codepoint as n2cp
reload(sys)  
sys.setdefaultencoding('utf8')
debug=0
tot=0
db = MySQLdb.connect("localhost","musicbrainz","musicbrainz","musicbrainz" )
c = db.cursor()
c.execute("SET NAMES utf8;") 
c.execute("SET CHARACTER SET utf8;")
file = open('Library.xml')
fields = [ 'Artist', 'Location', 'Album', 'Year' ]
dict = {}
artists = {}
def decodeHtmlentities(string):
    rc=-1
    try:
      entity_re = re.compile("&(#?)(\d{1,5}|\w{1,8});")
      def substitute_entity(match):
          ent = match.group(2)
          if match.group(1) == "#":
              return unichr(int(ent))
          else:
              cp = n2cp.get(ent)
              if cp:
                  return unichr(cp)
              else:
                  return match.group()
      rc=entity_re.subn(substitute_entity, string)[0]
    except:
      print sys.exc_info()
      pass
    return rc
def count_itunes_locations():
  tot=0
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
  nbr_loc=count_itunes_locations()
  print "Extracting " + str(nbr_loc) + " songs..."
  album='empty'
  artist='empty'
  partist='empty'
  pyear=0
  year=0
  pinc=0
  inc=0
  prog=0
  eta=0
  file.seek(0)
  estart = time.time()
  for line in file:
    if debug:
      if debug > 1:
        print line
    for field in fields:
      rs="<key>" + field + "</key><.*>(.*)</.*>"
      ctime = time.time()
      m = re.search(rs, line)
      if m:
        if field is not 'Location':
          dict.update({field:m.group(1)})
        if field is 'Location':
          prog+=1 
          sys.stdout.write("\rProgress %.2f%s [%d/%d] Running: %d s ETA: %d s" % (float(prog) / float(nbr_loc) * 100, '%', prog, nbr_loc, ctime - estart,  eta  ) )
          sys.stdout.flush()
          for f in fields:
            if f is not 'Location':
              try:
                if debug:
                  print dict[f]
              except:
                pass
          pdict=dict
          partist=artist
          palbum=album
          pyear=year
          artist=decodeHtmlentities(dict['Artist']).lower()
          album=decodeHtmlentities(dict['Album']).lower()
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
            pinc=inc
            inc=1
            if (partist != "unknown" and palbum != "unknown"):
              eartist=MySQLdb.escape_string(partist)
              ealbum=MySQLdb.escape_string(palbum)
              sql = "select present from musicbrainz where artist like '" + eartist + "' and name like '" + ealbum  +"';"
              if debug:
                print sql
              r=c.execute(sql)
              if c.rowcount:
                (result,)=c.fetchone()
                if (result != pinc):
                  sys.stdout.write("\n")
                  print("WARNING: %d != %d and corrected to %d for artist %s and album %s" % ( result, pinc, artists[ partist ][ palbum ]['inc'], eartist, ealbum ))
                  sql = "update musicbrainz set present=" + str(artists[ partist ][ palbum ]['inc']) + " where artist like '" + eartist + "' and name like '" + ealbum  +"';"
                  if debug:
                    sys.stdout.write("\n")
                    print sql
                  sys.stdout.flush()
                  r=c.execute(sql)
                  rall = c.fetchall()
              else:
                if (palbum != 'empty' and partist != 'empty'):
                  eartist=MySQLdb.escape_string(partist)
                  ename=MySQLdb.escape_string(palbum)
                  etype="Album"
                  eyear=pyear
                  sql = "insert into musicbrainz values(" + str(pinc) + ", '" + eartist + "','" + ename + "','" + etype + "','" + eyear + "',NULL);"
                  if debug:
                    sys.stdout.write("\n")
                    print sql
                    print("INFO: New album found = Count: %d Artist: %s Album: %s Year: %d" % ( pinc, eartist, ename, int(eyear) ))
                  sys.stdout.flush()
                  r=c.execute(sql)
                  rall = c.fetchall()
          else:
            inc+=1
            dict.update({'inc': inc})
                
tstart = time.time()
extract_from_itunes()
tend = time.time() - tstart
sys.stdout.write("\n\ndone in %ds\n" % (tend))
sys.stdout.flush()
