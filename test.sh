increment=1
if [[ $((increment % 2)) -eq 0 ]]; then
	echo "ola"          
fi
increment=2
if [[ $((increment % 2)) -eq 0 ]]; then
	echo "ola2"        
fi
