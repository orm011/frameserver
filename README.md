
# Frameserver
Has two goals: 
* serve you frames from video for visualization by offering a random access interface (web server through url or python class), which you can then access from your browser or embed in some page (eg a jupyter notebook)
* serve you frames from video sequentially for processing (eg through a pytorch dataset)
* In the sequential case, offer you ids for your frames in sequential mode so that you can store these and refer to them via the random access API later on if you need to.

Currently it only operates on .mp4 (it works for .tsv and .mov, but these container formats have the disadvantage that they are not chrome browser supported, which we currently also want, also, .mp4 have a convenient 'moov' index which makes random access possible without having to scan the video first). For convenience, we include some functions to re-mux the above into .mp4 (no re-encoding, just moving container). We found at least for .tsv, pyAV will not honor jumping to the a specific pts (may land farther in the future sometimes, and it is not easy to tell when this happens), so there needs to be a good reason for supporting other formats.

# Naming frames.
It seems like using the frame presentation timestamp (aka pts) is a good approach to naming frames, as it is supported for random access into .mp4 by libraries such as pyAV. It is an integer (unlike approx time), avoids ambiguity about what the index order means (it is not clear sequentully decoding frames get re-ordered wrt pts), and different videos have different frame-rates. 

We initially considered integer indexing as if the video were an array, it seemed desirable but it constantly feels like one is going against the grain of most tooling available.

The downside of using pts is that each video file has its own time unit, and this unit is needed to translate pts to seconds, and that it is not clear how to name the previous and next frames given the pts of a frame.  On the other hand, if one wants the frame '50 ms after the current one', indexing is not helpful either. 

## Installing
`pip install git+<>` should work.

## Using
(choose your own port)
`uvicorn -m frameserver.server --port 8500`

`wget http://localhost:8500/frame/path/to/video.mp4?pts=`
  
