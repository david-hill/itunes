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
fields = [ 'Artist', 'Location', 'Album', 'Year' ]
dict = {}
artists = {}

hours="272"

myid=0
def fetch_artist(artist,cptdone,artistcpt):
  myid=0
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

def fetch_releases(artist,myid,albumdone,cptalbum,stime):
  try:
    result = musicbrainzngs.browse_release_groups(myid, includes=["release-group-rels"], offset=0)
    max=result["release-group-count"]
  except:
    print sys.exc_info()
    max=-1
    pass
  offset=0
  cptartistalbum=0
  while (offset<max):
    try:
      result = musicbrainzngs.browse_release_groups(myid, includes=["release-group-rels"], offset=offset)
      max=result["release-group-count"]
      for release in result["release-group-list"]:
        ctime=time.time()
        try:
          if 'type' not in release.keys():
            release.update({'type': 'none'})
          if debug:
            print("{title} {date} ({type})".format(title=release["title"], type=release["type"], date=release["first-release-date"]))
          eartist=MySQLdb.escape_string(artist)
          ealbum=MySQLdb.escape_string(release["title"])
          if albumdone > 0:
            eta=int( float(ctime - stime) / float(albumdone) * (cptalbum - albumdone) )
          else:
            eta=0
          sql = "select present from musicbrainz where artist like '" + eartist + "' and name like '" + ealbum  +"' and type like '" + release["type"] + "';"
          if debug:
            print sql
          r=c.execute(sql)
          if c.rowcount:
            (result,)=c.fetchone()
            if debug:
              print sql
          else:
            year=release["first-release-date"]
            eyear=year[:4]
            sql = "insert into musicbrainz values(0, '" + eartist + "','" + ealbum + "','" + release["type"] + "','" + eyear + "', CURRENT_TIMESTAMP);"
            if debug:
              print sql
            r=c.execute(sql)
            rall = c.fetchall()
          albumdone+=1
          sys.stdout.write("\rProgress %.2f%s [%d/%d] Running: %d s ETA: %d s" % (float(albumdone) / float(cptalbum) * 100, '%',albumdone,cptalbum, ctime - stime, eta))
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

  return max

def count_albums():
  cptalbum=0
  cptdone=0
  results=0
  stime = time.time()
  sys.stdout.write("\rCounting albums...\n")
  sys.stdout.flush()
  sql = "select count(distinct artist) from musicbrainz where last_updated is null or last_updated < DATE_SUB(NOW(), INTERVAL " + hours + " HOUR);"
  r=c.execute(sql)
  (artistcpt,)=c.fetchone()
  if artistcpt:
    sql = "select artist from musicbrainz where last_updated is null or last_updated < DATE_SUB(NOW(), INTERVAL " + hours + " HOUR) group by artist;"
    r=c.execute(sql)
    results=c.fetchall()
    for result in results:
      myid=fetch_artist(result[0],cptdone,artistcpt)
      result = musicbrainzngs.browse_release_groups(myid,'' , offset=0)
      cptalbum+=result["release-group-count"]
      cptdone+=1
      ctime = time.time()
      eta=int( float(ctime - stime) / float(cptdone) * ( artistcpt - cptdone ) )
      sys.stdout.write("\rTotal Progress %.2f %s [%d/%d] Running: %d s ETA: %d s" % ( float(cptdone) / float(artistcpt) * 100, '%', cptdone, artistcpt, ctime - stime, eta))
      sys.stdout.flush()
  sys.stdout.write("done\n")
  sys.stdout.flush()
  return (results,artistcpt,cptalbum)

def sync_musicbrainz(results,artistcpt,cptalbum):
  mstart = time.time()
  cptdone=0
  albumdone=0
  if artistcpt:
    for result in results:
      myid=fetch_artist(result[0],cptdone,artistcpt)
      if myid:
        albumdone+=fetch_releases(result[0],myid,albumdone,cptalbum,mstart)
      cptdone+=1
      eartist=MySQLdb.escape_string(result[0])
      sql = "update musicbrainz set last_updated=CURRENT_TIMESTAMP where artist like '"+eartist+"';"
      r=c.execute(sql)

gstart=time.time()
(results, artistcpt, cptalbum)=count_albums()
sync_musicbrainz(results,artistcpt,cptalbum)
gend=time.time()
print("done in %d s" % (gend - gstart))


