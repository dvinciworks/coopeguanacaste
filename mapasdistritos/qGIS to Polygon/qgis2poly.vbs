
Function GetFileName()
'set the type of dialog box you want to use
'1 = Open
'2 = SaveAs
'3 = File Picker
'4 = Folder Picker
Const msoFileDialogOpen = 1

Set fso = CreateObject("Scripting.FileSystemObject")
Set objWord = CreateObject("Word.Application")
Set WshShell = CreateObject("WScript.Shell")

'where you want to start looking for files
'You could use a string like "C:\Somefolder\Somefolder\"
'I chose to use the desktop folder of whoever was running the script.  On Windows 7 it's "C:\Users\Username\Desktop\"
'Run "set" from a command prompt to see the available environment variables
strInitialPath = WshShell.ExpandEnvironmentStrings("%USERPROFILE%") & "\Desktop\"

'set the dialog box to open at the desired folder
objWord.ChangeFileOpenDirectory(strInitialPath)

With objWord.FileDialog(msoFileDialogOpen)
   'set the window title to whatever you want
   .Title = "Select the file to process"
   'I changed this to false because I'm working with one file at a time
   .AllowMultiSelect = False
   'Get rid of any existing filters
   .Filters.Clear
   'Show only the desired file types
   'for each desired group of file types, add a "Filters.Add" line with a different description and desired extensions
   'the dialog box will open using whichever filter is first
   'you can switch to a different filter from a drop-down list on the dialog box
   .Filters.Add "All Files", "*.*"
   .Filters.Add "Excel Files", "*.xls;*.xlsx"
   .Filters.Add "Text Files", "*.txt"
   .Filters.Add "Various Files", "*.xls;*.doc;*.vbs"
         
   '-1 = Open the file
   ' 0 = Cancel the dialog box
   '-2 = Close the dialog box
   'If objWord.FileDialog(msoFileDialogOpen).Show = -1 Then  'long form
   If .Show = -1 Then  'short form
      'Set how you want the dialog window to appear
      'it doesn't appear to do anything so it's commented out for now
      '0 = Normal
      '1 = Maximize
      '2 = Minimize
      'objWord.WindowState = 2

      'the Word dialog must be a collection object
      'even though I'm using one file, I had to use a For/Next loop
      '"File" returns a string containing the full path of the selected file
     
      'For Each File in objWord.FileDialog(msoFileDialogOpen).SelectedItems  'long form
      For Each File in .SelectedItems  'short form
         'Change the Word dialog object to a file object for easier manipulation
         Set objFile = fso.GetFile(File)
         'Display the full path to the file
         'WScript.Echo objFile.Path
         'Display the path to the folder that the file is in
         'WScript.Echo objFile.ParentFolder
         'Display just the name of the file
         'WScript.Echo objFile.Name
		 'Wscript.Echo objFile.Type
		 GetFileName = objFile.Path
      Next    
   Else 
   End If
End With

'Close Word
objWord.Quit
End Function


dim fso, path, pathAndName
path = GetFileName()
set fso = createobject("scripting.filesystemobject")
'getbase = fso.getbasename(path)
'getbase = fso.GetAbsolutePathName(path)
'getbase = fso.GetParentFolderName(path)
'msgbox getbase

pathAndName = fso.GetParentFolderName(path) & "\" & fso.getbasename(path)


ReshapePoly path,  _
            pathAndName & "_polyons.txt",  _
            pathAndName & "_attributes.txt",  _
            pathAndName & "_header.txt"





function ReshapePoly (strFile, strPoly, strAttr, strHead)

	Const ForAppending = 8
	Const ForReading = 1

    'Open the input file
    Set objFSO = CreateObject("Scripting.FileSystemObject")
    Set objReadFile = objFSO.OpenTextFile(strFile, ForReading, False)
	
	'create and open the polygon output file
    Set objPolyFile = objFSO.CreateTextFile(strPoly)
    objPolyFile.Close
    Set objPolyFile = objFSO.OpenTextFile (strPoly, ForAppending, True)
	
	'create and optn the attribute output file
    Set objAttrFile = objFSO.CreateTextFile(strAttr)
    objAttrFile.Close
    Set objAttrFile = objFSO.OpenTextFile (strAttr, ForAppending, True)
	
	'create and optn the attribute output file
    Set objHeadFile = objFSO.CreateTextFile(strHead)
    objHeadFile.Close
    Set objHeadFile = objFSO.OpenTextFile (strHead, ForAppending, True)
	objHeadFile.Write("Enter the column number you want added (0 to select none)")
	
	'setup variables
	dim strCharacter
	dim intHeader
	dim strHeader
	dim intPolygonID
	dim intSubPolygonID
	dim intPointID
	dim strPrevChar
	dim intMaxHeader
	strCharacter = ""
	strHeader = ""
	strPrevChar = ""
	intHeader = 0
	intPolygonID = 0
	intSubPolygonID = 0
	intPointID = 0
	intMaxHeader = 0
	
	'read the first line, the header line
	Do Until strCharacter = vbCr
	    'read one character at a time
        strCharacter = objReadFile.Read(1)
		
        If strCharacter = vbTab Then
		    intHeader = intHeader + 1
			objAttrFile.Write(vbTab)
			If intHeader > 0 Then
			    objHeadFile.Write(vbNewLine & intHeader & ". ")
			End If
        ElseIf strCharacter = vbCr Then
		    objAttrFile.Write(vbNewLine)
        Else
            objAttrFile.Write(strCharacter)
			If intHeader > 0 Then
			    objHeadFile.Write(strCharacter)
			End If
        End If
    Loop
	intMaxHeader = intHeader
	
	'write the header row to the polygon output file
	objPolyFile.Write("PolygonID" & vbTab & "SubPolygonID" & vbTab & "PointID" & vbTab)
	objPolyFile.Write("Longitude" & vbTab & "Latitude" & vbNewLine)
	
    Do Until objReadFile.AtEndOfStream
        strCharacter = objReadFile.Read(1)
        If strCharacter = "P" Then
            strCharacter = objReadFile.Read(8) 'to deal with "POLYGON(("
			intPolygonID = intPolygonID + 1
			intSubPolygonID = 1
			intPointID = 1
	        objPolyFile.Write(intPolygonID & vbTab & intSubPolygonID & vbTab & intPointID & vbTab)
	        objAttrFile.Write(intPolygonID & vbTab)
        ElseIf strCharacter = "M" Then
            strCharacter = objReadFile.Read(14) 'to deal with "MULTIPOLYGON((("
			intPolygonID = intPolygonID + 1
			intSubPolygonID = 1
			intPointID = 1
	        objPolyFile.Write(intPolygonID & vbTab & intSubPolygonID & vbTab & intPointID & vbTab)
	        objAttrFile.Write(intPolygonID & vbTab)
        ElseIf strCharacter = "(" or strCharacter = vbLf Then
		    'do nothing
        ElseIf strCharacter = " " Then
		    objPolyFile.Write(vbTab)
        ElseIf strCharacter = "," Then
            objPolyFile.Write(vbNewLine)
			intPointID = intPointID + 1
	        objPolyFile.Write(intPolygonID & vbTab & intSubPolygonID & vbTab & intPointID & vbTab)
        ElseIf strCharacter = ")" Then
		    If strCharacter <> strPrevChar Then
			    intSubPolygonID = intSubPolygonID + 1
			    intPointID = 0
			End If
	    ElseIf strCharacter = vbTab Then
		    Do Until strCharacter = vbCr or objReadFile.AtEndOfStream
                strCharacter = objReadFile.Read(1)
				If strCharacter = vbCr Then
				    objAttrFile.Write(vbNewLine)
				Else
                    objAttrFile.Write(strCharacter)
				End If
            Loop
			If Not objReadFile.AtEndOfStream Then
			    objPolyFile.Write(vbNewLine)
			End If
        Else
            objPolyFile.Write(strCharacter)
        End If
		strPrevChar = strCharacter
    Loop
	
	'close files
    objReadFile.Close
    objPolyFile.Close
    objAttrFile.Close
	objHeadFile.Close
	
    'Use the Head file
    Set objHeadFile = objFSO.OpenTextFile(strHead, ForReading, False)
	strOptions = objHeadFile.ReadAll 
    objHeadFile.Close 
	objFSO.DeleteFile(strHead) 
	
	'Offer user option to add column
	intHeader = InputBox(strOptions,"Select Column to Add",0)
    If IsNumeric(intHeader) Then
        intHeader = intHeader * 1
		if intHeader > 0 and intHeader <= intMaxHeader Then
			
			'rename the polygon file made
			objFSO.MoveFile strPoly , strPoly & ".read"
			
			'open the files made for reading
			Set objReadFile = objFSO.OpenTextFile (strPoly & ".read", ForReading, False)
			Set objAttrFile = objFSO.OpenTextFile (strAttr, ForReading, False)
			
			'create and open the polygon output file
			Set objPolyFile = objFSO.CreateTextFile(strPoly)
			objPolyFile.Close
			Set objPolyFile = objFSO.OpenTextFile (strPoly, ForAppending, True)
			
			'read in the Attr header row
			strAttrLine = objAttrFile.Readline
			arrAttrValues = Split(strAttrLine, vbTab)
			strAttrPolyID = arrAttrValues(0)
			strAttrHeader = arrAttrValues(intHeader)
			
			'read in the Poly header row, adding additional header
			strReadLine = objReadFile.Readline
			objPolyFile.WriteLine(strReadLine & vbTab & strAttrHeader)
			
			Do Until objReadFile.AtEndOfStream
				strReadLine = objReadFile.Readline
				arrReadValues = Split(strReadLine, vbTab)
				strPolyID = arrReadValues(0)
				If strPolyID = strAttrPolyID Then
					objPolyFile.WriteLine(strReadLine & vbTab & strAttrValue)
				Else
					strAttrLine = objAttrFile.Readline
					arrAttrValues = Split(strAttrLine, vbTab)
					strAttrPolyID = arrAttrValues(0)
					strAttrValue = arrAttrValues(intHeader)
					objPolyFile.WriteLine(strReadLine & vbTab & strAttrValue)
				End If					
			Loop
			
			objReadFile.Close
			objAttrFile.Close
			objPolyFile.Close
			objFSO.DeleteFile(strPoly & ".read") 
			
		    WScript.Echo "Files complete."
		Else
		    WScript.Echo "Not adding column. Files complete."
		End If
    Else
        WScript.Echo "Numbers only. Not adding column. Files complete."
    End If
	
end function





