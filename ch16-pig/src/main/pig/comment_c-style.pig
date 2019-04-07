/* 
 * Opis programu obejmujący
 * kilka wierszy.
 */
A = LOAD 'input/pig/join/A';
B = LOAD 'input/pig/join/B';
C = JOIN A BY $0, /* Ignorowane */ B BY $1;
DUMP C;