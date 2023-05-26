Output sendit.gif

Require echo

Set Shell "bash"
Set FontSize 32
Set Width 1400
Set Height 600

Type `echo "hello world" > body.txt`
Enter
Sleep 500ms
Type `sendit -subject="hello" -body="body.txt"`
Enter
Sleep 500ms
Tab@500ms 2
Sleep 1s
Enter
Sleep 1s
Type `nats stream view`
Enter
Sleep 1s
Enter
Sleep 5s
