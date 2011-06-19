import json, sys, getpass, os, urllib
from fusion_table.authorization.clientlogin import ClientLogin
from fusion_table.sql.sqlbuilder import SQL
import fusion_table.ftclient as ftclient

_TID = 946168

def _find_slug( collection, slug ):
	for object in collection:
		if object["slug"] == slug:
			return object

def main():
	#os.chdir(os.path.dirname(__file__)) #Working path fix for NppExec, remove in production
	#Create Fusion Tables connection
	username = sys.argv[1]
	password = getpass.getpass("Enter your password: ")

	token = ClientLogin().authorize(username, password)
	ft_client = ftclient.ClientLoginFTClient(token)

	#Load data
	offices = json.load( open( "./offices.json", "r" ) )
	reports = json.load( open( "./reports.json", "r" ) )
	topics = json.load( open( "./topics.json", "r" ) )
	issues = json.load( open( "./issues.json", "r" ) )
	
	#Denormalize data
	flat_records = []
	i = 1
	for issue in issues:
		#issue["topic"] = _find_slug( topics, issue["topic"] )["name"].encode("utf-8")
		issue["topic"] = urllib.quote( _find_slug( topics, issue["topic"] )["name"].encode("utf-8") )
		#issue["text"] = issue["text"].encode("utf-8")
		issue["text"] = urllib.quote( issue["text"].encode("utf-8") )
		inspected = issue["inspected"]
		followups = issue["followups"]
		#issue["slug"] = issue["slug"].encode("utf-8")
		issue["slug"] = urllib.quote( issue["slug"].encode("utf-8") )
		#issue["report"] = issue["report"].encode("utf-8")
		issue["report"] = urllib.quote( issue["report"].encode("utf-8") )
		for followup in followups:
			#issue["followups"] = followup.encode("utf-8")
			issue["followups"] = urllib.quote( followup.encode( "utf-8" ) )
			for inspectee in inspected:
				#issue["inspected"] = _find_slug( offices, inspectee)["name"].encode("utf-8")
				issue["inspected"] = urllib.quote( _find_slug( offices, inspectee)["name"].encode("utf-8") )
				issue["id"] = i
				i += 1
				flat_records.append( issue )
	
	#Load data to the Fusion Table
	for record in flat_records:
		ft_client.query(SQL().insert(_TID, record))

if __name__ == "__main__":
	main()