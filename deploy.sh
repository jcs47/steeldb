rm config/currentView
cp -v ~/NetBeansProjects/MerkleTree/dist/MerkleTree.jar lib/
rm -R bin/*
ant
cp -v bin/SteelDB.jar ../RUBiS/Servlet_HTML/WEB-INF/lib/
cp -v ~/NetBeansProjects/MerkleTree/dist/MerkleTree.jar ../RUBiS/Servlet_HTML/WEB-INF/lib/
