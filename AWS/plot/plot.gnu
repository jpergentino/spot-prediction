reset
set terminal png size 1280, 345
set output output_file
unset key

#set palette model RGB

set palette defined ( 0 "red", 2 "green"  )
set palette defined ( 0 "#336699", 2 "green"  )
set palette defined ( 0 "#00008B", 2 "green" )
set palette defined ( 0 "#B22222", 1 "orange", 2 "green"  )


set palette defined ( 0 "black", 2 "white"  )
set palette defined ( 0 "#B22222", 1 "orange", 2 "yellow", 3 "green"  )
set palette defined ( 0 "black", 1 "green"  )
set palette defined ( 0 "#191970", 1 "green"  )
set palette defined ( 0 "#191970", 1 "#32CD32"  )
set palette defined ( 0 "white", 1 "gray"  )
set palette defined ( 0 "black", 1 "gray", 2 "white"  )
set palette defined ( 0 "#B22222", 2 "#008B8B", 3 "green"  )
set palette defined ( 0 "#B22222", 2 "green"  )
set palette defined ( 0 "white", 1 "#32CD32"  )
set palette defined ( 0 "white", 2 "black"  )
set palette defined ( 0 "white", 1 "#90EE90"  )
  


set palette defined ( 0 "white", 1 "#32CD32"  )
set palette defined ( 0 "#B22222", 1 "orange", 2 "yellow", 3 "green"  )



set cbrange [0:]
set bmargin at screen 0.1

#linhas
set tics scale 1
set grid ytics xtics

set pm3d map
set pm3d interpolate 0,0

#set format cb "%g %%" 

splot input_file matrix rowheaders columnheaders 
