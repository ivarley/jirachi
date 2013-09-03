## Jirachi: simple analysis of JIRA tickets for an open source project.

I did this project as a part of preparing for a talk at HBaseCon2013, "1500 JIRAs in 20 minutes". What I needed was a way to pull down all the JIRA tickets (and comments, and attachments) that matched a given search (in my case, all tickets closed in one year), and then import them into a SQL datase for my analysis. In my case, for the SQL database, I used HBase with Phoenix on top, which worked pretty well.

The heart of the analysis I did was really simple categorization or "tagging", in which I created a list of simple SQL pattern matching expressions for each concept I wanted to tag. For example, for tagging those JIRA issues that were part of test code, the set of matchers I used was: 

	"isTest" : [
		"test",
		"junit",
		"hbase-it"
	]

Each of these is evaluated in a SQL statement like: '... WHERE summary like %{matcher}%'. These can also contain SQL wildcards like '%', and it adds a value to a column named after the tag. There's also an exclude list that's evaluated like '... AND summary NOT like %{matcher}%'.

If you want to use this code for doing something similar, feel free. The matcher(and match excluder) lists are in json files, and the other relevant stuff is in a properties file. Please ping me if you end up using it for anything interesting: [@thefutureian](https://twitter.com/thefutureian)
