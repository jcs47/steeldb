cp -v ../../MerkleTree/dist/MerkleTree.jar lib/
rm -R bin/*
ant
cp -v bin/SteelDB.jar ../RUBiS/Servlet_HTML/WEB-INF/lib/
cp -v ../../MerkleTree/dist/MerkleTree.jar ../RUBiS/Servlet_HTML/WEB-INF/lib/
