import re
import MySQLdb
import sys  
import musicbrainzngs
from htmlentitydefs import name2codepoint as n2cp
reload(sys)  
sys.setdefaultencoding('utf8')
debug=0
tot=0
db = MySQLdb.connect("localhost","musicbrainz","musicbrainz","musicbrainz" )
c = db.cursor()
c.execute("SET NAMES utf8;") 
c.execute("SET CHARACTER SET utf8;")
fields = [ 'Artist', 'Location', 'Album', 'Year' ]
dict = {}
artists = {}

myid=0
def fetch_artist(artist,cptdone,artistcpt):
  myid=0
  print("%s\t\tTotal Progress %.2f %s [%d/%d]" % ( artist, float(cptdone) / float(artistcpt) * 100, '%', cptdone, artistcpt))
  musicbrainzngs.set_useragent('itunesync', '0.0.1', 'contact=daweidave@hotmail.com')
  try:
    result = musicbrainzngs.search_artists(artist=artist, type="group")
    for artist in result['artist-list']:
      try:
        if debug:
          print(u"{id}: {name}".format(id=artist['id'], name=artist["name"]))
        if not myid:
          myid=artist["id"]
      except:
        print sys.exc_info()
        pass
  except:
    pass
  return myid
def fetch_releases(artist,myid):
  try:
    result = musicbrainzngs.browse_release_groups(myid, includes=["release-group-rels"], offset=0)
    max=result["release-group-count"]
  except:
    print sys.exc_info()
    max=-1
    pass
  offset=0
  while (offset<max):
    try:
      result = musicbrainzngs.browse_release_groups(myid, includes=["release-group-rels"], offset=offset)
      max=result["release-group-count"]
      for release in result["release-group-list"]:
        try:
          if 'type' not in release.keys():
            release.update({'type': 'none'})
          if debug:
            print("{title} {date} ({type})".format(title=release["title"], type=release["type"], date=release["first-release-date"]))
          eartist=MySQLdb.escape_string(artist)
          ealbum=MySQLdb.escape_string(release["title"])
          sql = "select present from musicbrainz where artist like '" + eartist + "' and name like '" + ealbum  +"' and type like '" + release["type"] + "';"
          if debug:
            print sql
          r=c.execute(sql)
          if c.rowcount:
            (result,)=c.fetchone()
            sql = "update musicbrainz set last_updated=CURRENT_TIMESTAMP where artist like '" + eartist + "' and name like '" + ealbum + "' and type like '" + release["type"] + "' and year like '" + release["first-release-date"] + "';"
            if debug:
              print sql
            r=c.execute(sql)
            rall = c.fetchall()
          else:
            sql = "insert into musicbrainz values(0, '" + eartist + "','" + ealbum + "','" + release["type"] + "','" + release["first-release-date"] + "', CURRENT_TIMESTAMP);"
            if debug:
              print sql
            r=c.execute(sql)
            rall = c.fetchall()
          sys.stdout.write("\rProgress %.2f%s [%d/%d]" % (float(offset) / float(max) * 100, '%',offset,max))
          sys.stdout.flush()
        except:
          print sys.exc_info()
          pass
      offset+=25
      if (offset > max):
        offset=max
    except:
      print sys.exc_info()
      pass
  sys.stdout.write("\rProgress %.2f%s [%d/%d]\n" % (float('1') / float('1') * 100, '%', offset, max))
  sys.stdout.flush()

def sync_musicbrainz():
  cptdone=0
  sql = "select count(distinct artist) from musicbrainz where last_updated is null or last_updated < DATE_SUB(NOW(), INTERVAL 30 DAY);"
  r=c.execute(sql)
  (artistcpt,)=c.fetchone()
  if artistcpt:
    sql = "select artist from musicbrainz where last_updated is null or last_updated < DATE_SUB(NOW(), INTERVAL 30 DAY) group by artist;"
    r=c.execute(sql)
    results=c.fetchall()
    for result in results:
      myid=fetch_artist(result[0],cptdone,artistcpt)
      if myid:
        fetch_releases(result[0],myid)
      cptdone+=1
    sql = "select count(distinct artist) from musicbrainz where last_updated is null or last_updated < DATE_SUB(NOW(), INTERVAL 30 DAY);"
    r=c.execute(sql)
    (artistcpt,)=c.fetchone()
    print("WARNING: %d artists were not updated!  Cleaning them..." % (artistcpt) )
    sql = "update musicbrainz set last_updated=CURRENT_TIMESTAMP where last_updated is null or last_updated < DATE_SUB(NOW(), INTERVAL 30 DAY);"
    r=c.execute(sql)
sync_musicbrainz()
